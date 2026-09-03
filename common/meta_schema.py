"""Build a Spark SCHEMA, an Iceberg column-DDL list, and a row-builder
function from one field-spec list, so a table's column set has exactly one
source of truth instead of three hand-written, independently-maintained
lists that can drift out of alignment with each other.

Used by every job in jobs/ -- both the dimension tables (34-62 fields per
entity) and the performance tables, where it's the mechanism for turning
"what fields to pull" (a business decision, kept duplicated per job -- see
each job's own field-spec list and its comments) into the three parallel
artifacts Spark/Iceberg need.

Note this module imports pyspark.sql.types directly, unlike the rest of
common/ (and unlike the sibling pinterest_ads_pipeline project's common/,
which keeps pyspark imports out of its shared modules entirely, confined to
each job script). That's fine in the real Glue runtime -- pyspark is always
on the path there -- but means any local unit test against this module (or
anything importing it) needs pyspark importable too, real or stubbed.
"""

import json

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Valid `kind` values for a field spec (api_field_name, column_name, kind):
#   "string"    -- passed through as-is
#   "double"    -- cast via float()
#   "bigint"    -- cast via int(float(...))  (handles "123" and "123.0" alike)
#   "boolean"   -- passed through as-is (Meta returns real JSON booleans)
#   "date"      -- stored as the raw "YYYY-MM-DD" string Meta returns (e.g.
#                  date_start/date_stop), cast to a real date via a SQL CAST
#                  at merge/replace time
#   "timestamp" -- stored as the raw ISO-8601 string Meta returns, cast to a
#                  real timestamp via a SQL CAST at merge/replace time (Spark
#                  parses ISO-8601-with-offset natively)
#   "json"      -- serialized with json.dumps(); None stays None


def build_table(field_specs: list, id_column: str = "ad_account_id"):
    """field_specs: ordered list of (api_field_name, column_name, kind).

    Every table built this way gets an `id_column` (default "ad_account_id")
    prepended and an `ingested_at` timestamp appended, since every job in
    this project needs both.

    Returns (schema, iceberg_columns, to_row):
      schema: a pyspark.sql.types.StructType
      iceberg_columns: list of (name, sql_type) or (name, sql_type, select_expr)
        tuples, ready for meta_iceberg.upsert()/replace_table()
      to_row: to_row(id_value, obj, ingested_at) -> tuple, matching schema's
        column order exactly, by construction
    """
    schema_fields = [StructField(id_column, StringType(), False)]
    iceberg_columns = [(id_column, "string")]

    for _, column_name, kind in field_specs:
        if kind == "string":
            schema_fields.append(StructField(column_name, StringType(), True))
            iceberg_columns.append((column_name, "string"))
        elif kind == "double":
            schema_fields.append(StructField(column_name, DoubleType(), True))
            iceberg_columns.append((column_name, "double"))
        elif kind == "bigint":
            schema_fields.append(StructField(column_name, LongType(), True))
            iceberg_columns.append((column_name, "bigint"))
        elif kind == "boolean":
            schema_fields.append(StructField(column_name, BooleanType(), True))
            iceberg_columns.append((column_name, "boolean"))
        elif kind == "date":
            schema_fields.append(StructField(column_name, StringType(), True))
            iceberg_columns.append((column_name, "date", f"CAST({column_name} AS date)"))
        elif kind == "timestamp":
            schema_fields.append(StructField(column_name, StringType(), True))
            iceberg_columns.append((column_name, "timestamp", f"CAST({column_name} AS timestamp)"))
        elif kind == "json":
            schema_fields.append(StructField(column_name, StringType(), True))
            iceberg_columns.append((column_name, "string"))
        else:
            raise ValueError(f"unknown field kind {kind!r} for {column_name}")

    schema_fields.append(StructField("ingested_at", TimestampType(), False))
    iceberg_columns.append(("ingested_at", "timestamp"))

    def to_row(id_value, obj: dict, ingested_at) -> tuple:
        values = [id_value]
        for api_field, _, kind in field_specs:
            val = obj.get(api_field)
            if kind == "double":
                val = float(val) if val not in (None, "") else None
            elif kind == "bigint":
                val = int(float(val)) if val not in (None, "") else None
            elif kind == "json":
                val = json.dumps(val) if val is not None else None
            # string / boolean / timestamp: passed through as-is
            values.append(val)
        values.append(ingested_at)
        return tuple(values)

    return StructType(schema_fields), iceberg_columns, to_row
