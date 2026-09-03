"""Meta access token handling, backed by AWS Secrets Manager.

Unlike Pinterest, this deliberately has no token-refresh logic. It's built
against a Meta System User access token, which -- unlike a standard User/Page
OAuth token (~60 day expiry) -- does not expire once generated in Business
Manager, so there's no refresh_token exchange to perform on every run; the
job just reads the stored token and uses it directly.

If you end up using a standard (expiring) OAuth token instead, this module
is the one place that needs to grow a refresh step -- see
pinterest_auth.py in the sibling pinterest_ads_pipeline project for the
shape that takes (exchange a refresh token, detect rotation, write the new
value back to Secrets Manager).
"""

import json
import logging

import boto3

logger = logging.getLogger(__name__)


def get_secret(secret_name: str, region: str) -> dict:
    """Fetch and JSON-decode a secret: {"access_token": "..."}.

    Also accepts an optional "app_secret" key -- not used for authenticating
    ordinary calls, but Meta's "appsecret_proof" mechanism (an HMAC of the
    access token using the app secret) is recommended for server-to-server
    calls to guard against token replay. Not implemented here; add it to
    meta_http.py if your app requires it (Business Settings > System Users >
    "Require App Secret" can make this mandatory).
    """
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def get_access_token(secret_name: str, region: str) -> str:
    creds = get_secret(secret_name, region)
    return creds["access_token"]
