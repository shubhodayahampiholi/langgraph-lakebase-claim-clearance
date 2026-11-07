# Databricks notebook source
# MAGIC %md
# MAGIC ### Parse the corpus
# MAGIC Stage 1: parse every PDF in the volume once with `ai_parse_document` and store the
# MAGIC result. The table is created only if it does not exist, so re-running costs nothing.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS claim_clearance.knowledge.parsed_documents AS
    SELECT
        replace(element_at(split(path, '/'), -1), '.pdf', '') AS doc_id,
        path,
        CAST(ai_parse_document(content, map('version', '2.0')) AS STRING) AS parsed_json
    FROM READ_FILES('/Volumes/claim_clearance/knowledge/corpus/*.pdf', format => 'binaryFile')
""")
print("parsed_documents ready")

# COMMAND ----------

import json

rows = spark.sql(
    "SELECT doc_id, parsed_json FROM claim_clearance.knowledge.parsed_documents ORDER BY doc_id"
).collect()

for r in rows:
    parsed = json.loads(r["parsed_json"])
    elements = parsed["document"]["elements"]
    counts = {}
    for e in elements:
        counts[e["type"]] = counts.get(e["type"], 0) + 1
    print(r["doc_id"], "| error:", parsed.get("error_status"), "| elements:", len(elements), "|", counts)
