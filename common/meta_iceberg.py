"""Generic Iceberg write helpers shared by every job in jobs/.

Two patterns, for two different kinds of data:
- upsert(): CREATE TABLE IF NOT EXISTS + MERGE INTO, for time-series fact
  data. Most jobs pull a metric that can be revised for a date after the
  fact (attribution windows), so re-running a job for an overlapping date
  range must overwrite existing rows for that entity+date, not duplicate
  them or leave stale ones.
- replace_table(): CREATE OR REPLACE TABLE ... AS SELECT, for the dimension
  tables (campaign/ad set/ad metadata), fetched in full on every run. MERGE
  would be wrong here: it only ever adds or updates rows, so a
  campaign/ad set/ad deleted on Meta's side would linger forever. A full
  replace makes the table exactly match Meta's current data on every run.
"""

import logging

logger = logging.getLogger(__name__)


def upsert(spark, df, full_table_name: str, columns: list, key_columns: list,
           partition_expr: str, temp_view_name: str) -> None:
    """Create the target table if needed, then MERGE df into it.

    columns: ordered list of (name, sql_type) tuples defining the table
      schema, or (name, sql_type, select_expr) when the source column needs
      a transform (e.g. casting the string stat_date column to `date`).
      select_expr defaults to `name` when omitted.
    key_columns: column names forming the merge key, e.g.
      ["ad_account_id", "ad_id", "stat_date"].
    partition_expr: Iceberg PARTITIONED BY expression, e.g. "days(stat_date)".
    temp_view_name: name to register df under via createOrReplaceTempView.
    """
    df.createOrReplaceTempView(temp_view_name)

    ddl_lines = []
    select_lines = []
    for col in columns:
        name, sql_type = col[0], col[1]
        select_expr = col[2] if len(col) > 2 else name
        ddl_lines.append(f"{name} {sql_type}")
        select_lines.append(name if select_expr == name else f"{select_expr} AS {name}")

    ddl_sql = ",\n            ".join(ddl_lines)
    select_sql = ",\n                ".join(select_lines)
    on_clause = " AND ".join(f"target.{k} = source.{k}" for k in key_columns)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            {ddl_sql}
        )
        USING iceberg
        PARTITIONED BY ({partition_expr})
    """)

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING (
            SELECT
                {select_sql}
            FROM {temp_view_name}
        ) AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    logger.info("Merge complete into %s", full_table_name)


def replace_table(spark, df, full_table_name: str, columns: list, temp_view_name: str) -> None:
    """Overwrite full_table_name with exactly the contents of df.

    columns: ordered list of (name, sql_type) tuples defining the table
      schema, or (name, sql_type, select_expr) when a source column needs a
      transform. select_expr defaults to `name` when omitted.
    temp_view_name: name to register df under via createOrReplaceTempView.
    """
    df.createOrReplaceTempView(temp_view_name)

    select_lines = []
    for col in columns:
        name = col[0]
        select_expr = col[2] if len(col) > 2 else name
        select_lines.append(name if select_expr == name else f"{select_expr} AS {name}")

    select_sql = ",\n            ".join(select_lines)

    spark.sql(f"""
        CREATE OR REPLACE TABLE {full_table_name}
        USING iceberg
        AS SELECT
            {select_sql}
        FROM {temp_view_name}
    """)

    logger.info("Replaced %s with %d row(s)", full_table_name, df.count())
