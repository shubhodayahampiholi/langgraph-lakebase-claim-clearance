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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stage 2: one chunk per section, built from the stored parse
# MAGIC `ai_prep_search` dropped the section headings from the retrieved text, so findings
# MAGIC could not cite rule numbers. We group the parsed elements by section ourselves.

# COMMAND ----------

import json, re

TABLE = "claim_clearance.knowledge.chunks"


def clean(text):
    return " ".join(text.split())  # collapse line breaks and repeated spaces


def table_to_text(html):
    """<table> -> one line per row: 'column: value | column: value'."""
    rows = [re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", r, flags=re.S)
            for r in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.S)]
    rows = [[clean(re.sub(r"<[^>]+>", "", c)) for c in r] for r in rows]
    header, body = rows[0], rows[1:]
    return "\n".join(" | ".join(f"{h}: {v}" for h, v in zip(header, r)) for r in body)


def sections_from(elements):
    """Group the ordered elements into (section header, [body pieces]) pairs."""
    title = None
    sections = [["(preamble)", []]]
    for el in elements:
        kind, content = el["type"], el.get("content") or ""
        if kind == "title":
            title = clean(content)
        elif kind == "section_header":
            sections.append([clean(content), []])
        elif kind == "text":
            sections[-1][1].append(clean(content))
        elif kind == "table":
            sections[-1][1].append(table_to_text(content))
        else:
            raise ValueError(f"unhandled element type: {kind}")
    return title, [(h, b) for h, b in sections if b and h != "(preamble)"]

# COMMAND ----------

if spark.catalog.tableExists(TABLE):
    print(f"{TABLE} already exists, skipping (drop the table to rebuild it)")
else:
    manifest = {
        m["doc_id"]: m
        for m in json.load(open("/Volumes/claim_clearance/knowledge/corpus/manifest.json"))
    }
    docs = spark.sql(
        "SELECT doc_id, parsed_json FROM claim_clearance.knowledge.parsed_documents ORDER BY doc_id"
    ).collect()

    rows = []
    for d in docs:
        doc_id = d["doc_id"]
        tags = manifest[doc_id]  # fails loudly if a document has no tags
        parsed = json.loads(d["parsed_json"])
        if parsed.get("error_status"):
            raise RuntimeError(f"{doc_id}: {parsed['error_status']}")

        title, sections = sections_from(parsed["document"]["elements"])
        for i, (header, body) in enumerate(sections):
            rows.append({
                "chunk_key": f"{doc_id}:{i}",
                "doc_id": doc_id,
                "section_index": i,
                "section_header": header,
                "scope": tags["scope"],
                "product": tags["product"],
                "doc_type": tags["doc_type"],
                "chunk_text": f"{title}\n{header}\n\n" + "\n".join(body),
            })
        print(doc_id, "->", len(sections), "chunk(s)")

    spark.createDataFrame(rows).write.saveAsTable(TABLE)
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"wrote {len(rows)} chunks to {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

df = spark.table(TABLE)
rows = df.orderBy("doc_id", "section_index").collect()
print("chunks:", len(rows), "| distinct chunk_key:", df.select("chunk_key").distinct().count())
print("untagged chunks:", df.filter("scope IS NULL OR scope = ''").count())

for r in rows:
    print(f"{r['chunk_key']:12} scope={r['scope']:7} {len(r['chunk_text']):5} chars | {r['section_header']}")

def find(doc_id, header_start):
    return next(r for r in rows if r["doc_id"] == doc_id and r["section_header"].startswith(header_start))

res = find("study-1", "Results")["chunk_text"]
print("\nstudy-1 Results chunk has all six numbers:", all(n in res for n in ["58%", "74%", "81%", "31%", "38%", "40%"]))
print("study-1 Results chunk has the significance sentence:", "statistically significant" in res)
print("study-1 Limitations chunk has the no-data sentence:", "provides no data" in find("study-1", "Limitations")["chunk_text"])

timing = next(r for r in rows if r["doc_id"] == "uk-rules" and "directly measured the effect" in r["chunk_text"])
print("UK timing rule sits under the header:", timing["section_header"])

print("\n--- study-1 Results chunk ---")
print(res)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh: re-chunk only the documents that changed
# MAGIC Uses the stored parse, so it makes no AI calls. Deletes the old chunk rows of the listed
# MAGIC documents and appends the new ones, so the other documents and the search index are
# MAGIC left in place. Safe to repeat.

# COMMAND ----------

import json

TABLE = "claim_clearance.knowledge.chunks"
CHANGED = ["us-rules", "in-rules"]  # fixed list written here, never taken from outside

manifest = {
    m["doc_id"]: m
    for m in json.load(open("/Volumes/claim_clearance/knowledge/corpus/manifest.json"))
}

rows = []
for doc_id in CHANGED:
    tags = manifest[doc_id]
    parsed_json = spark.sql(
        "SELECT parsed_json FROM claim_clearance.knowledge.parsed_documents WHERE doc_id = :d",
        args={"d": doc_id},
    ).first()["parsed_json"]
    parsed = json.loads(parsed_json)
    if parsed.get("error_status"):
        raise RuntimeError(f"{doc_id}: {parsed['error_status']}")

    title, sections = sections_from(parsed["document"]["elements"])
    for i, (header, body) in enumerate(sections):
        rows.append({
            "chunk_key": f"{doc_id}:{i}",
            "doc_id": doc_id,
            "section_index": i,
            "section_header": header,
            "scope": tags["scope"],
            "product": tags["product"],
            "doc_type": tags["doc_type"],
            "chunk_text": f"{title}\n{header}\n\n" + "\n".join(body),
        })
    print(doc_id, "->", sum(1 for r in rows if r["doc_id"] == doc_id), "chunk(s)")

for doc_id in CHANGED:
    spark.sql(f"DELETE FROM {TABLE} WHERE doc_id = '{doc_id}'")
spark.createDataFrame(rows).write.mode("append").saveAsTable(TABLE)

# COMMAND ----------

df = spark.table(TABLE)
print("chunks:", df.count(), "| distinct chunk_key:", df.select("chunk_key").distinct().count())
print("per document:", {r["doc_id"]: r["count"] for r in df.groupBy("doc_id").count().collect()})

chunks = {r["chunk_key"]: " ".join(r["chunk_text"].split()) for r in df.collect()}
us = " ".join(t for k, t in chunks.items() if k.startswith("us-rules:"))
ind = " ".join(t for k, t in chunks.items() if k.startswith("in-rules:"))
print("us Rule 1 new sentence in chunks:", "does not require that a trial measured the specific time point" in us)
print("us Rule 2 new sentence in chunks:", "is not, by itself, overstatement" in us)
print("in Rule 2 new sentence in chunks:", "is not relevant to this rule" in ind)
print("headers kept whole:", sorted(r["section_header"] for r in df.filter("doc_id = 'us-rules'").collect()))
