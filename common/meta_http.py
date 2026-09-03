"""HTTP retry/backoff wrapper shared by every Meta Graph API call.

Same retry contract as the Pinterest pipeline's http helper (429 honoring
Retry-After, 5xx backoff) for consistency across both projects. Meta's Graph
API returns errors in a JSON body ({"error": {"message", "code",
"error_subcode", "error_user_msg", "fbtrace_id"}}) even for a bare 500 --
but requests' default HTTPError message (raise_for_status()'s
"500 Server Error: Internal Server Error for url: ...") never includes that
body, so the actual reason gets silently discarded unless something goes
looking for it. describe_meta_error()/the raise in request_with_backoff()
below attach it to the exception message instead, so a job's traceback shows
Meta's real message/code/fbtrace_id, not just the generic HTTP reason phrase.
"""

import json
import logging
import time

import requests

from meta_config import BATCH_SIZE, GRAPH_API_BASE, INITIAL_BACKOFF_SECONDS, MAX_RETRIES

logger = logging.getLogger(__name__)


def describe_meta_error(resp: requests.Response) -> str:
    """Extract Meta's structured error detail from a response body, if any.
    Returns "" if the body isn't JSON, isn't a JSON object, or has no
    "error" key -- e.g. a plain-text 5xx from an intermediate proxy rather
    than Meta's API itself, or a *batch* response, whose body is a JSON
    array of per-sub-request results rather than an object. (This function
    runs on every response, successes included, so it has to tolerate every
    body shape the API returns, not just error-shaped ones.)
    """
    try:
        body = resp.json()
    except ValueError:
        return ""
    if not isinstance(body, dict):
        return ""

    err = body.get("error")
    if not err:
        return ""

    parts = []
    if err.get("message"):
        parts.append(err["message"])
    if err.get("error_user_msg"):
        parts.append(f"user_message={err['error_user_msg']}")
    if err.get("type"):
        parts.append(f"type={err['type']}")
    if err.get("code") is not None:
        parts.append(f"code={err['code']}")
    if err.get("error_subcode") is not None:
        parts.append(f"error_subcode={err['error_subcode']}")
    if err.get("fbtrace_id"):
        parts.append(f"fbtrace_id={err['fbtrace_id']}")
    return " | Meta error: " + ", ".join(parts) if parts else ""


def _raise_with_detail(resp: requests.Response) -> None:
    detail = describe_meta_error(resp)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(str(e) + detail, response=resp) from None


def request_with_backoff(method: str, url: str, params: dict = None,
                          max_retries: int = MAX_RETRIES,
                          initial_backoff: int = INITIAL_BACKOFF_SECONDS,
                          **kwargs) -> requests.Response:
    """requests.request() with retry on 429 (honoring Retry-After) and 5xx.

    Raises (with Meta's structured error detail appended -- see
    describe_meta_error()) for any other error status, and after exhausting
    max_retries.
    """
    backoff = initial_backoff
    resp = None
    for attempt in range(1, max_retries + 1):
        resp = requests.request(method, url, params=params, timeout=60, **kwargs)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", backoff))
            logger.warning("Rate limited by Meta Graph API, sleeping %ss (attempt %s/%s)%s",
                            retry_after, attempt, max_retries, describe_meta_error(resp))
            time.sleep(retry_after)
            backoff *= 2
            continue
        if resp.status_code >= 500:
            logger.warning("Meta Graph API %s error, retrying in %ss (attempt %s/%s)%s",
                            resp.status_code, backoff, attempt, max_retries, describe_meta_error(resp))
            time.sleep(backoff)
            backoff *= 2
            continue
        _raise_with_detail(resp)
        return resp
    _raise_with_detail(resp)  # last attempt's error, if we fell through
    return resp


def get_all_pages(url: str, params: dict, access_token: str) -> list:
    """Follow Graph API cursor pagination (paging.next) and return every
    item in "data" across all pages.

    access_token is sent as an `access_token` query parameter, not an
    `Authorization: Bearer` header -- every example in Meta's own docs
    (the general Graph API guide, the Insights guide, and the breakdowns
    reference, all fetched 2026-08-20) uses the query parameter form
    exclusively; none show a Bearer header. Meta's Graph API is also known
    to surface access-token problems as HTTP 400 (OAuthException), not 401,
    which is why a bad-auth failure here looks like a "malformed request"
    rather than an auth error.

    Meta's paging model: each response has {"data": [...], "paging": {
    "cursors": {"after": ...}, "next": "<full url for the next page>"}}.
    Absence of "next" (not absence of "cursors") is what signals the last
    page -- a short final page can still carry a cursors object. Meta
    echoes the access_token into the "next" URL it returns, so subsequent
    pages stay authenticated without re-adding it.
    """
    items = []
    next_url = url
    next_params = dict(params) if params else {}
    next_params["access_token"] = access_token

    while next_url:
        resp = request_with_backoff("GET", next_url, params=next_params)
        body = resp.json()
        items.extend(body.get("data", []))

        next_url = body.get("paging", {}).get("next")
        next_params = None  # "next" is a complete URL (access_token included); don't re-append params

    return items


def batch_request(sub_requests: list, access_token: str) -> list:
    """Send up to BATCH_SIZE sub-requests as one Graph API batch call.

    sub_requests: list of dicts, each {"method": "GET"|"POST",
      "relative_url": "act_123/insights", and optionally "body":
      "urlencoded=form&data=here"} (POST/PUT only). relative_url is relative
      to the *versioned* base (GRAPH_API_BASE), so "act_123/insights"
      resolves to /{version}/act_123/insights.

    Returns a list the same length and order as sub_requests, one entry per
    sub-request: {"code": <int http status>, "body": <parsed JSON dict>,
    "error": <describe-style string or "">}.

    Batch semantics worth knowing (all per Meta's batch-requests doc):
    - Hard limit of 50 sub-requests per batch; callers should chunk with
      meta_config.BATCH_SIZE.
    - The access token goes at the top level; sub-requests inherit it.
    - **Each sub-request still counts individually toward rate limits.** A
      batch saves HTTP round trips and wall-clock time, NOT quota -- don't
      reach for batching as a throttle fix on its own.
    - Individual sub-requests can fail without failing the batch: the
      overall call returns HTTP 200 and the failure shows up as that entry's
      own "code" (e.g. 400/403/500) with an error payload in its body. So a
      caller MUST check each entry's code rather than assuming success.
    - Each entry's "body" arrives as a JSON *string*, not a nested object,
      so it needs a second json.loads() -- done here.
    """
    if len(sub_requests) > BATCH_SIZE:
        raise ValueError(
            f"batch_request got {len(sub_requests)} sub-requests, "
            f"Meta's limit is {BATCH_SIZE} -- chunk before calling"
        )

    resp = request_with_backoff(
        "POST",
        GRAPH_API_BASE,
        data={"batch": json.dumps(sub_requests), "access_token": access_token},
    )

    results = []
    for i, entry in enumerate(resp.json()):
        # A null entry means that sub-request timed out inside the batch --
        # Meta documents this as possible for slow operations.
        if entry is None:
            results.append({"code": 504, "body": {}, "error": "batch sub-request timed out (null entry)"})
            continue

        code = entry.get("code")
        raw_body = entry.get("body") or "{}"
        try:
            body = json.loads(raw_body)
        except ValueError:
            body = {}

        error = ""
        if code is None or code >= 400:
            err = body.get("error") or {}
            parts = [str(p) for p in (
                err.get("message"), err.get("error_user_msg"),
                f"type={err['type']}" if err.get("type") else None,
                f"code={err['code']}" if err.get("code") is not None else None,
                f"error_subcode={err['error_subcode']}" if err.get("error_subcode") is not None else None,
                f"fbtrace_id={err['fbtrace_id']}" if err.get("fbtrace_id") else None,
            ) if p]
            error = f"HTTP {code}" + (f" -- {', '.join(parts)}" if parts else "")
            logger.warning("Batch sub-request %d/%d failed: %s (%s %s)",
                            i + 1, len(sub_requests), error,
                            sub_requests[i].get("method"), sub_requests[i].get("relative_url"))

        results.append({"code": code, "body": body, "error": error})

    return results


def batched(seq: list, size: int = BATCH_SIZE):
    """Yield successive `size`-length chunks of seq, for feeding batch_request()."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]
