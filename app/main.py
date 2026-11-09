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

    identity = WorkspaceClient().current_user.me().user_name
    async with await connect() as conn:
        cur = await conn.execute(
            "SELECT claim_id, status, regions FROM claims ORDER BY submitted_at DESC LIMIT 20"
        )
        return identity, await cur.fetchall()


try:
    identity, rows = asyncio.run(read_claims())
    st.success(f"Connected to Lakebase as {identity}")
    st.dataframe([{"claim_id": r[0], "status": r[1], "regions": ", ".join(r[2])} for r in rows])
except Exception as e:
    st.error(f"{type(e).__name__}: {e}")
