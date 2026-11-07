# Databricks notebook source
# MAGIC %md
# MAGIC ### Create the search endpoint and index
# MAGIC Run notebooks 00 to 02 first. This creates an AI Search endpoint and a Delta Sync index
# MAGIC over the chunks table, then waits until a filtered search returns the expected chunks.
# MAGIC Free Edition allows one search endpoint per account. Creating the index can take 10 to 20 minutes.

# COMMAND ----------

import json, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, EndpointType, PipelineType, VectorIndexType,
)

dbutils.widgets.text("endpoint_name", "claim-clearance-search")
dbutils.widgets.text("index_name", "claim_clearance.knowledge.chunks_index")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
INDEX = dbutils.widgets.get("index_name")
SOURCE_TABLE = "claim_clearance.knowledge.chunks"

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check the source table

# COMMAND ----------

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(f"{SOURCE_TABLE} does not exist. Run notebook 02 (parse and chunk) first.")
props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {SOURCE_TABLE}").collect()}
if props.get("delta.enableChangeDataFeed") != "true":
    raise RuntimeError(f"{SOURCE_TABLE} needs change data feed on. Re-run notebook 02.")
print(f"{SOURCE_TABLE}: {spark.table(SOURCE_TABLE).count()} chunks, change data feed on")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The endpoint (created only if missing)

# COMMAND ----------

existing = [e.name for e in w.vector_search_endpoints.list_endpoints()]
print("existing search endpoints:", existing)

if ENDPOINT_NAME in existing:
    print("using the existing endpoint:", ENDPOINT_NAME)
elif existing:
    raise RuntimeError(
        f"This workspace already has a search endpoint {existing}, and Free Edition allows one. "
        "Put its name in the 'endpoint_name' box and re-run this cell."
    )
else:
    w.vector_search_endpoints.create_endpoint_and_wait(name=ENDPOINT_NAME, endpoint_type=EndpointType.STANDARD)
    print("created:", ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The index (created only if missing)
# MAGIC Triggered sync: the index updates only when you ask (`sync_index`), which keeps cost low.
# MAGIC Databricks computes the embeddings from `chunk_text`; `scope`, `product` and `doc_type`
# MAGIC come along as filterable columns.

# COMMAND ----------

try:
    w.vector_search_indexes.get_index(INDEX)
    print("index already exists:", INDEX)
except Exception as e:
    print("index not found, creating it:", str(e)[:100])
    w.vector_search_indexes.create_index(
        name=INDEX,
        endpoint_name=ENDPOINT_NAME,
        primary_key="chunk_key",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=SOURCE_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(name="chunk_text", embedding_model_endpoint_name="databricks-gte-large-en")
            ],
        ),
    )
    print("index creation requested:", INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait until the index answers a filtered search
# MAGIC The status message is not reliable on its own (it can say ready while still syncing), so
# MAGIC this waits for the content: five chunks for each region and ten global ones.

# COMMAND ----------

EXPECTED = {"UK": 5, "US": 5, "IN": 5, "global": 10}

def count(scope):
    res = w.vector_search_indexes.query_index(
        index_name=INDEX, columns=["chunk_key"], query_text="claims standard",
        filters_json=json.dumps({"scope": scope}), num_results=20,
    )
    return len(res.result.data_array or [])

for attempt in range(40):
    try:
        got = {scope: count(scope) for scope in EXPECTED}
    except Exception as e:
        got = f"not answering yet ({str(e)[:80]})"
    print(f"check {attempt + 1}:", got)
    if got == EXPECTED:
        print("the index is ready")
        break
    time.sleep(30)
else:
    raise RuntimeError("The index did not return the expected chunks after 20 minutes. Look at its page in Catalog Explorer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test search: a region filter returns only that region
# MAGIC The unfiltered search is here to show why the filter matters.

# COMMAND ----------

def show(filters=None):
    kwargs = {"filters_json": json.dumps(filters)} if filters else {}
    res = w.vector_search_indexes.query_index(
        index_name=INDEX, columns=["chunk_key", "scope", "section_header"],
        query_text="timing claim measured at 15 minutes", num_results=5, **kwargs,
    )
    for row in res.result.data_array:
        print(f"  {row[-1]:.3f}  {row[0]:12} scope={row[1]:7} {row[2]}")

print("no filter:"); show()
print("scope = UK:"); show({"scope": "UK"})
