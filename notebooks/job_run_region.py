# Databricks notebook source
# This notebook is what a Databricks Job runs for one region of one claim.
# Parameters: claim_id, region, and optionally endpoint.

# MAGIC %pip install -U "databricks-langchain[memory]" langgraph

# COMMAND ----------

# The binary build of psycopg crashes on Databricks serverless (FIPS self-test failure).
# MAGIC %pip uninstall -y psycopg-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys

dbutils.widgets.text("claim_id", "")
dbutils.widgets.text("region", "")
dbutils.widgets.text("endpoint", "")

claim_id = dbutils.widgets.get("claim_id")
region = dbutils.widgets.get("region")
endpoint = dbutils.widgets.get("endpoint")
if not claim_id or not region:
    raise ValueError("claim_id and region are required")

# Must be set before claim_clearance is imported.
if endpoint:
    os.environ["CLAIM_CLEARANCE_ENDPOINT"] = endpoint

# Find the repo's src folder from this notebook's own location (notebooks/ -> repo root).
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(nb_path))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from claim_clearance.runner import start_region

# COMMAND ----------

result = await start_region(claim_id, region)
print(f"{claim_id}:{region} -> {result}")
dbutils.notebook.exit(result)
