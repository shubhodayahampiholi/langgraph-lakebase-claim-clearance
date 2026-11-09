import asyncio
import os
import sys

import streamlit as st

# The package lives in ../src (the App is deployed from the repo root).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

st.title("Claim clearance")
st.caption("Step 1: connection check. This page only reads.")

st.write("Endpoint received:", os.environ.get("CLAIM_CLEARANCE_ENDPOINT", "(not set)"))
try:
    st.write("Signed-in user (proxy header):", st.context.headers.get("X-Forwarded-Email", "(none)"))
except Exception as e:
    st.write("Could not read request headers:", repr(e))


async def read_claims():
    from databricks.sdk import WorkspaceClient
    from claim_clearance.db import connect

    sdk_identity = WorkspaceClient().current_user.me().user_name
    async with await connect() as conn:
        cur = await conn.execute("SELECT current_user")
        pg_role = (await cur.fetchone())[0]
        try:
            cur = await conn.execute(
                "SELECT claim_id, status, regions FROM claims ORDER BY submitted_at DESC LIMIT 20"
            )
            return sdk_identity, pg_role, await cur.fetchall(), None
        except Exception as e:
            return sdk_identity, pg_role, [], f"{type(e).__name__}: {e}"


try:
    sdk_identity, pg_role, rows, error = asyncio.run(read_claims())
    st.write("Databricks identity of the App:", sdk_identity)
    st.write("Postgres role it connected as:", pg_role)
    if error:
        st.error(error)
    else:
        st.success("Connected to Lakebase")
        st.dataframe([{"claim_id": r[0], "status": r[1], "regions": ", ".join(r[2])} for r in rows])
except Exception as e:
    st.error(f"{type(e).__name__}: {e}")
