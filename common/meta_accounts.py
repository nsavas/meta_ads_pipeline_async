"""Ad account and entity (campaigns / ad sets / ads) discovery.

Meta's discovery flow is a bit different from Pinterest's single
GET /ad_accounts call:
  1. GET /me identifies the token itself -- for a System User access token,
     the returned "id" *is* the System User's ID (confirmed against Meta's
     live Graph API reference docs, https://developers.facebook.com/docs/graph-api/,
     on 2026-08-19).
  2. GET /{system_user_id}/assigned_ad_accounts lists every ad account that
     System User has been assigned in Business Manager.

Unlike Pinterest, Meta requires an explicit `fields` parameter on every list
call -- there's no "just give me everything" default -- so list_entities()
takes a `fields` argument rather than returning a fixed full object.
"""

import json
import logging

from meta_config import DEFAULT_PAGE_SIZE, GRAPH_API_BASE
from meta_http import get_all_pages, request_with_backoff

logger = logging.getLogger(__name__)


def get_self_id(access_token: str) -> str:
    """GET /me -- for a System User token, this is the System User's own ID.

    /me returns a single object, not a paginated list, so this calls
    request_with_backoff() directly rather than going through
    get_all_pages() (which is built for list edges).
    """
    resp = request_with_backoff(
        "GET",
        f"{GRAPH_API_BASE}/me",
        params={"fields": "id", "access_token": access_token},
    )
    return resp.json()["id"]


def list_ad_accounts(access_token: str) -> list:
    """Discover every ad account this System User token can see:
    GET /me to find the System User's own ID, then page through
    /{system_user_id}/assigned_ad_accounts.

    Returns account IDs in Meta's native "act_123456789" form -- that's the
    form every downstream /{ad_account_id}/campaigns (etc.) call needs as
    its path segment, so no prefix bookkeeping is needed by callers.
    """
    self_id = get_self_id(access_token)
    items = get_all_pages(
        f"{GRAPH_API_BASE}/{self_id}/assigned_ad_accounts",
        {"fields": "id", "limit": DEFAULT_PAGE_SIZE},
        access_token,
    )
    accounts = [item["id"] for item in items]
    logger.info("Discovered %d ad account(s) via the Meta Graph API", len(accounts))
    return accounts


def list_entities(ad_account_id: str, access_token: str, entity_path: str, fields: list,
                   page_size: int = DEFAULT_PAGE_SIZE, filtering: list = None) -> list:
    """Page through /{ad_account_id}/{entity_path} collecting the full
    entity object (whichever fields were requested) for every entity in
    the account.

    ad_account_id: Meta's "act_123456789" form (as returned by list_ad_accounts).
    entity_path: "campaigns", "adsets", or "ads".
    fields: list of field names to request -- Meta has no "return everything"
      default, unlike Pinterest's list endpoints.
    page_size: defaults to DEFAULT_PAGE_SIZE (100), fine for the ID-only
      calls list_entity_ids() makes. Callers requesting many/wide fields per
      object (see da_mkt_meta_dimension_datalake.py) should pass
      meta_config.DETAIL_PAGE_SIZE instead -- see that constant's docstring
      for why (Meta's cost-based throttle on large full-object pages).
    filtering: optional list of Marketing API filter dicts, e.g.
      [{"field": "campaign.created_time", "operator": "GREATER_THAN_OR_EQUAL",
      "value": 1700000000}]. JSON-encoded and sent as the `filtering` query
      parameter -- this filters which entities are returned by the edge
      itself (unlike `date_preset`/`time_range`, which only scope computed
      stats fields and don't affect which objects come back). See
      da_mkt_meta_dimension_datalake.py's CREATED_TIME filtering notes.
    """
    params = {"fields": ",".join(fields), "limit": page_size}
    if filtering:
        params["filtering"] = json.dumps(filtering)
    items = get_all_pages(
        f"{GRAPH_API_BASE}/{ad_account_id}/{entity_path}",
        params,
        access_token,
    )
    logger.info("Ad account %s: found %d %s", ad_account_id, len(items), entity_path)
    return items


def list_entity_ids(ad_account_id: str, access_token: str, entity_path: str) -> list:
    """Same as list_entities(), but only fetches `id` -- for jobs that only
    need IDs to loop over (e.g. to call each entity's own /insights edge),
    not the full entity object.
    """
    entities = list_entities(ad_account_id, access_token, entity_path, fields=["id"])
    return [entity["id"] for entity in entities]


def resolve_ad_account_ids(args: dict, access_token: str) -> list:
    """Return the list of ad account IDs to pull.

    Normally this discovers every account the token's System User has been
    assigned. If AD_ACCOUNT_IDS was passed as a job parameter, treat it as
    an allowlist filter over the discovered accounts (rather than trusting
    it blindly) so a stale/typo'd ID doesn't silently pull zero accounts.

    Accepts AD_ACCOUNT_IDS with or without the "act_" prefix and normalizes
    it on, since that's easy to get wrong typing IDs by hand.
    """
    discovered = list_ad_accounts(access_token)

    requested = args.get("AD_ACCOUNT_IDS")
    if not requested:
        return discovered

    requested_ids = [
        a.strip() if a.strip().startswith("act_") else f"act_{a.strip()}"
        for a in requested.split(",") if a.strip()
    ]
    discovered_set = set(discovered)
    missing = [a for a in requested_ids if a not in discovered_set]
    if missing:
        logger.warning(
            "AD_ACCOUNT_IDS included account(s) not visible to this token, skipping: %s",
            missing,
        )

    filtered = [a for a in requested_ids if a in discovered_set]
    logger.info("Restricting run to %d of %d discovered ad account(s)", len(filtered), len(discovered))
    return filtered
