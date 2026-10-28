"""Plain SQL connection to Lakebase, authenticated with a short-lived token.

No password is stored anywhere: the token is generated from your Databricks login.
Note: install plain `psycopg`, not `psycopg[binary]`. The binary build bundles its
own OpenSSL and crashes with a FIPS self-test failure on Databricks serverless.
"""
import psycopg
from databricks.sdk import WorkspaceClient

from claim_clearance.config import ENDPOINT


async def connect():
    w = WorkspaceClient()
    host = w.postgres.get_endpoint(name=ENDPOINT).status.hosts.host
    token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
    user = w.current_user.me().user_name
    return await psycopg.AsyncConnection.connect(
        host=host, dbname="databricks_postgres",
        user=user, password=token, sslmode="require",
    )
