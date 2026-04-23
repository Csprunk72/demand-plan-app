"""
Snowflake connection helper for the Demand Plan Dashboard.

Auth methods (via SNOWFLAKE_AUTH_METHOD env var):
  - externalbrowser : Nike SSO via browser popup (local dev)
  - pat             : Programmatic Access Token (Databricks deployment)
  - password        : Username/password fallback

Credentials:
  - account, warehouse, user, role from env vars
  - PAT / password from Databricks Secrets with env var fallback
"""
from __future__ import annotations

import base64
import logging
import os
from decimal import Decimal

import pandas as pd
import snowflake.connector

logger = logging.getLogger(__name__)

SF_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT", "nike-nike_aws_us_west_2")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "EDA_MPO_ZOOMANALYTICS_PROD")
SF_USER      = os.environ.get("SNOWFLAKE_USER", "CSPRU1")
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "DF_CSPRU1")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "DA_DSM_SCANALYTICS_PROD")
SF_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "INTEGRATED")
SF_AUTH      = os.environ.get("SNOWFLAKE_AUTH_METHOD", "externalbrowser")

_SECRETS_SCOPE = "demand-plan-secrets"
_PAT_KEY       = "snowflake-pat"
_PASSWORD_KEY  = "snowflake-password"


def _get_secret(key: str) -> str | None:
    env_map = {"snowflake-pat": "SNOWFLAKE_PAT", "snowflake-password": "SNOWFLAKE_PASSWORD"}
    env_val = os.environ.get(env_map.get(key, ""))
    if env_val:
        return env_val
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        resp = w.secrets.get_secret(_SECRETS_SCOPE, key)
        if resp.value:
            return base64.b64decode(resp.value).decode("utf-8")
    except Exception:
        pass
    return None


def get_connection() -> snowflake.connector.SnowflakeConnection:
    params: dict = dict(
        account=SF_ACCOUNT,
        user=SF_USER,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

    method = SF_AUTH
    if method == "pat":
        token = _get_secret(_PAT_KEY)
        if not token:
            raise RuntimeError(
                f"Snowflake PAT not found. Set SNOWFLAKE_PAT env var or store in "
                f"Databricks Secrets (scope={_SECRETS_SCOPE}, key={_PAT_KEY})."
            )
        params["authenticator"] = "programmatic_access_token"
        params["token"] = token
    elif method == "externalbrowser":
        params["authenticator"] = "externalbrowser"
    else:
        pw = _get_secret(_PASSWORD_KEY)
        if not pw:
            raise RuntimeError(
                f"Snowflake password not found. Set SNOWFLAKE_PASSWORD env var or store in "
                f"Databricks Secrets (scope={_SECRETS_SCOPE}, key={_PASSWORD_KEY})."
            )
        params["password"] = pw

    logger.info("Connecting to Snowflake (method=%s, account=%s, user=%s)",
                method, SF_ACCOUNT, SF_USER)
    return snowflake.connector.connect(**params)


def query_dataframe(sql: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            df = cur.fetch_pandas_all()
        finally:
            cur.close()
    finally:
        conn.close()

    for col in df.columns:
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
            if isinstance(sample, Decimal):
                df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
