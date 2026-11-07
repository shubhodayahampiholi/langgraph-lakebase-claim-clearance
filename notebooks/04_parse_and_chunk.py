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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: chunk with ai_prep_search and attach our tags

# COMMAND ----------

import json

TABLE = "claim_clearance.knowledge.chunks"

if spark.catalog.tableExists(TABLE):
    print(f"{TABLE} already exists, skipping (drop the table to rebuild it)")
else:
    manifest = {
        m["doc_id"]: m
        for m in json.load(open("/Volumes/claim_clearance/knowledge/corpus/manifest.json"))
    }

    docs = spark.sql(
        "SELECT doc_id FROM claim_clearance.knowledge.parsed_documents ORDER BY doc_id"
    ).collect()

    rows = []
    for d in docs:
        doc_id = d["doc_id"]
        tags = manifest[doc_id]  # fails loudly if a document has no tags

        out = spark.sql(
            "SELECT CAST(ai_prep_search(PARSE_JSON(parsed_json)) AS STRING) AS j "
            "FROM claim_clearance.knowledge.parsed_documents WHERE doc_id = :d",
            args={"d": doc_id},
        ).first()["j"]
        result = json.loads(out)
        if result.get("error_status"):
            raise RuntimeError(f"{doc_id}: {result['error_status']}")

        for c in result["document"]["contents"]:
            position = int(c["chunk_position"])
            rows.append({
                "chunk_key": f"{doc_id}:{position}",
                "chunk_id": c["chunk_id"],
                "doc_id": doc_id,
                "chunk_position": position,
                "scope": tags["scope"],
                "product": tags["product"],
                "doc_type": tags["doc_type"],
                "chunk_to_retrieve": " ".join(c["chunk_to_retrieve"].split()),
                "chunk_to_embed": c["chunk_to_embed"],  # left exactly as returned
            })
        print(doc_id, "->", sum(1 for r in rows if r["doc_id"] == doc_id), "chunk(s)")

    spark.createDataFrame(rows).write.saveAsTable(TABLE)
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"wrote {len(rows)} chunks to {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify: read the chunks table back

# COMMAND ----------

df = spark.table(TABLE)
rows = df.collect()
print("chunks:", len(rows), "| distinct chunk_key:", df.select("chunk_key").distinct().count())
print("untagged chunks:", df.filter("scope IS NULL OR scope = ''").count())

for r in sorted(rows, key=lambda r: r["chunk_key"]):
    lines = r["chunk_to_embed"].splitlines()
    title = next((l for l in lines if l.startswith("Title:")), "")
    sections = next((l for l in lines if l.startswith("Sections:")), "")
    print(f"\n{r['chunk_key']} | scope={r['scope']} | retrieve={len(r['chunk_to_retrieve'])} chars")
    print("  ", title)
    print("  ", sections[:200])

by_doc = {r["doc_id"]: r["chunk_to_retrieve"] for r in rows}
print("\nstudy-1 table numbers present:", all(n in by_doc["study-1"] for n in ["58%", "74%", "81%", "31%", "38%", "40%"]))
print("study-2 India line present:", "53%" in by_doc["study-2"])
print("uk Rule 2 present:", "directly measured the effect at or before the time point claimed" in by_doc["uk-rules"])
print("change data feed:", spark.sql(f"SHOW TBLPROPERTIES {TABLE}").filter("key = 'delta.enableChangeDataFeed'").collect())
