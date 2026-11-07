# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup
# MAGIC Run this once. It installs the packages, checks that your Lakebase compute accepts
# MAGIC connections, and creates the Unity Catalog objects and the Lakebase tables the
# MAGIC other notebooks use. Everything is `IF NOT EXISTS`, so running it again is safe.
# MAGIC
# MAGIC Before you start: create a Lakebase project in your workspace (Lakebase, then create
# MAGIC a project) and note its endpoint path:
# MAGIC `projects/<project>/branches/<branch>/endpoints/<endpoint>`.

# COMMAND ----------

# MAGIC %md
# MAGIC The binary build of psycopg crashes on Databricks serverless (FIPS self-test failure), so remove it.

# COMMAND ----------

# MAGIC %pip uninstall -y psycopg-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys

dbutils.widgets.text("endpoint", "")
dbutils.widgets.dropdown("enable_if_disabled", "no", ["no", "yes"])
endpoint = dbutils.widgets.get("endpoint")
if not endpoint:
    raise ValueError("Fill in the endpoint box: projects/<project>/branches/<branch>/endpoints/<endpoint>")
os.environ["CLAIM_CLEARANCE_ENDPOINT"] = endpoint  # must be set before importing claim_clearance

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(nb_path))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from claim_clearance.config import ENDPOINT
print("ready | endpoint:", ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check that the Lakebase endpoint accepts connections
# MAGIC After a long idle period an endpoint can be left disabled. A disabled endpoint still shows as
# MAGIC IDLE, and connections are refused. If that happens, set `enable_if_disabled` to `yes` and re-run this cell.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, FieldMask

w = WorkspaceClient()
ep = w.postgres.get_endpoint(name=ENDPOINT)
print("disabled:", ep.status.disabled, "| state:", ep.status.current_state)

if ep.status.disabled:
    if dbutils.widgets.get("enable_if_disabled") != "yes":
        raise RuntimeError(
            "The Lakebase endpoint is disabled, so connections are refused. Set the "
            "'enable_if_disabled' box to 'yes' and re-run this cell to clear the flag."
        )
    w.postgres.update_endpoint(
        name=ENDPOINT,
        endpoint=Endpoint(
            name=ENDPOINT,
            spec=EndpointSpec(endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE, disabled=False),
        ),
        update_mask=FieldMask(field_mask=["spec.disabled"]),  # changes this one field only
    ).wait()
    ep = w.postgres.get_endpoint(name=ENDPOINT)
    print("after the change | disabled:", ep.status.disabled, "| state:", ep.status.current_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unity Catalog: catalog, schema and volume for the corpus

# COMMAND ----------

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS claim_clearance")
except Exception as e:
    raise RuntimeError(
        "Could not create the catalog 'claim_clearance'. If your workspace does not allow it, "
        "use an existing catalog instead and change the catalog name in notebooks 01, 02 and 03. "
        f"Original error: {str(e)[:200]}"
    )
spark.sql("CREATE SCHEMA IF NOT EXISTS claim_clearance.knowledge")
spark.sql("CREATE VOLUME IF NOT EXISTS claim_clearance.knowledge.corpus")
print("ready: claim_clearance.knowledge + volume 'corpus'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lakebase: the checkpointer's tables, the store's tables, and ours

# COMMAND ----------

from databricks_langchain import AsyncCheckpointSaver
from claim_clearance.db import connect
from claim_clearance.claims import create_claims_table
from claim_clearance.tracker import create_regions_table
from claim_clearance.memory import make_store

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    await checkpointer.setup()      # LangGraph's checkpoint tables
async with make_store() as store:
    await store.setup()             # the long-term memory tables, including the vector table
async with await connect() as conn:
    await create_claims_table(conn)
    await create_regions_table(conn)
print("tables ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

EXPECTED = {
    "claims", "claim_regions",
    "checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations",
    "store", "store_vectors", "store_migrations", "vector_migrations",
}

async with await connect() as conn:
    cur = await conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    found = {r[0] for r in await cur.fetchall()}

print("tables in Lakebase (public):", sorted(found))
print("missing:", sorted(EXPECTED - found) or "none")
