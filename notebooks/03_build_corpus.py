# Databricks notebook source
# MAGIC %md
# MAGIC ### Build the corpus
# MAGIC Turns the markdown files in `data/corpus/` into PDFs in a Unity Catalog volume,
# MAGIC plus a `manifest.json` holding each document's tags (scope, product, type).

# COMMAND ----------

# MAGIC %pip install reportlab

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, re, json, shutil

CATALOG, SCHEMA, VOLUME = "claim_clearance", "knowledge", "corpus"
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# The repo's data/corpus folder, found from this notebook's own location.
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(nb_path))
source_dir = f"/Workspace{repo_root}/data/corpus"
print("reading from:", source_dir)
print("files:", sorted(os.listdir(source_dir)))

# COMMAND ----------

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (ListFlowable, ListItem, Paragraph,
                                SimpleDocTemplate, Spacer, Table, TableStyle)

styles = getSampleStyleSheet()


def split_frontmatter(text):
    """The block between the first two '---' lines holds 'key: value' tags."""
    _, block, body = text.split("---", 2)
    meta = {}
    for line in block.strip().splitlines():
        key, value = line.split(":", 1)
        meta[key.strip()] = value.strip()
    return meta, body.strip()


def inline(text):
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)  # **bold**


def markdown_to_story(body):
    story, para, bullets, rows = [], [], [], []

    def flush():
        nonlocal para, bullets, rows
        if para:
            story.append(Paragraph(inline(" ".join(para)), styles["BodyText"]))
            story.append(Spacer(1, 6))
            para = []
        if bullets:
            items = [ListItem(Paragraph(inline(b), styles["BodyText"])) for b in bullets]
            story.append(ListFlowable(items, bulletType="bullet"))
            story.append(Spacer(1, 6))
            bullets = []
        if rows:
            data = [[Paragraph(inline(c), styles["BodyText"]) for c in r] for r in rows]
            table = Table(data, hAlign="LEFT")
            table.setStyle(TableStyle([
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ]))
            story.append(table)
            story.append(Spacer(1, 8))
            rows = []

    for raw in body.splitlines():
        line = raw.rstrip()
        if line.startswith("|"):                       # a table row
            if para or bullets:
                flush()
            cells = [c.strip() for c in line.strip("|").split("|")]
            if not all(set(c) <= set("-: ") for c in cells):   # skip the |---|---| row
                rows.append(cells)
        elif line.startswith("- "):                    # a bullet
            if para or rows:
                flush()
            bullets.append(line[2:].strip())
        elif line.startswith("  ") and bullets:        # a wrapped bullet continues
            bullets[-1] += " " + line.strip()
        elif line.startswith("#"):                     # a heading
            flush()
            level = len(line) - len(line.lstrip("#"))
            style = "Title" if level == 1 else "Heading2"
            story.append(Paragraph(inline(line.lstrip("#").strip()), styles[style]))
        elif not line.strip():                         # a blank line ends the block
            flush()
        else:                                          # plain text, joined into one paragraph
            if bullets or rows:
                flush()
            para.append(line.lstrip("> ").strip())
    flush()
    return story

# COMMAND ----------

manifest = []
for filename in sorted(os.listdir(source_dir)):
    if not filename.endswith(".md"):
        continue
    with open(os.path.join(source_dir, filename)) as f:
        meta, body = split_frontmatter(f.read())

    # Build in /tmp first, then copy: writing a PDF straight into a volume is less reliable.
    tmp_path = f"/tmp/{meta['doc_id']}.pdf"
    SimpleDocTemplate(tmp_path, pagesize=A4, title=meta["doc_id"]).build(markdown_to_story(body))
    shutil.copy(tmp_path, f"{volume_path}/{meta['doc_id']}.pdf")

    manifest.append(meta)
    print("wrote", f"{meta['doc_id']}.pdf", "| scope:", meta["scope"])

with open(f"{volume_path}/manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)

print("\nvolume now holds:", sorted(os.listdir(volume_path)))
