# Databricks notebook source
# MAGIC %md
# MAGIC ### Run a claim end to end
# MAGIC Run notebooks 00 to 03 first. This submits a claim, lets the system review it in each
# MAGIC selected region, shows what each reviewer would see, then plays the reviewers' answers.
# MAGIC
# MAGIC Fill in the boxes at the top, then run the cells in order. **Don't re-run the cell that
# MAGIC submits the answers**, because each answer is final. To try another claim, change the
# MAGIC `claim_id` box.
# MAGIC
# MAGIC By default regions run inside this notebook (about a minute for three regions). To run them
# MAGIC as Databricks Jobs instead, see the optional section at the end.

# COMMAND ----------

# MAGIC %pip install -U "databricks-langchain[memory]" langgraph

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
dbutils.widgets.text("search_index", "claim_clearance.knowledge.chunks_index")
dbutils.widgets.text("claim_id", "DEMO-001")
dbutils.widgets.text("claim_text", "Clinically proven to relieve pain in 15 minutes")
dbutils.widgets.dropdown("product", "PainRelief Tablets", ["PainRelief Tablets"])
dbutils.widgets.dropdown("medium", "TV", ["TV", "Social", "Print"])
dbutils.widgets.text("regions", "UK,US,IN")
dbutils.widgets.dropdown("starter", "in-process", ["in-process", "job"])
dbutils.widgets.text("region_job_id", "")  # only for starter = job

endpoint = dbutils.widgets.get("endpoint")
if not endpoint:
    raise ValueError("Fill in the endpoint box: projects/<project>/branches/<branch>/endpoints/<endpoint>")

# These must be set before claim_clearance is imported.
os.environ["CLAIM_CLEARANCE_ENDPOINT"] = endpoint
os.environ["CLAIM_CLEARANCE_SEARCH_INDEX"] = dbutils.widgets.get("search_index")
if dbutils.widgets.get("region_job_id"):
    os.environ["CLAIM_CLEARANCE_REGION_JOB_ID"] = dbutils.widgets.get("region_job_id")

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(nb_path))
sys.path.insert(0, f"/Workspace{repo_root}/src")

import asyncio, re, time
from databricks_langchain import AsyncCheckpointSaver
from langgraph.types import Command

from claim_clearance.config import ENDPOINT, MODEL_ENDPOINT
from claim_clearance.db import connect
from claim_clearance.claims import create_claims_table, create_claim, get_claim
from claim_clearance.tracker import create_regions_table, list_regions
from claim_clearance.intent import MAX_CLAIM_CHARS
from claim_clearance.knowledge import REGIONS
from claim_clearance.child_graph import build_child_graph
from claim_clearance.parent_graph import build_parent_graph
from claim_clearance.runner import start_region
from claim_clearance.launcher import start_region_job
from claim_clearance.memory import make_store, lesson_namespace

print("ready | endpoint:", ENDPOINT, "| model:", MODEL_ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Validate the inputs, then submit the claim
# MAGIC In the finished system the App does this at the door. A claim that fails here is never submitted.

# COMMAND ----------

claim_id = dbutils.widgets.get("claim_id").strip()
claim_text = dbutils.widgets.get("claim_text").strip()
regions = list(dict.fromkeys(r.strip() for r in dbutils.widgets.get("regions").split(",") if r.strip()))

if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", claim_id):
    raise ValueError("claim_id must be 1 to 64 characters: letters, digits, '.', '_' or '-' (no ':')")
if not 1 <= len(claim_text) <= MAX_CLAIM_CHARS:
    raise ValueError(f"claim_text must be 1 to {MAX_CLAIM_CHARS} characters")
if not regions or any(r not in REGIONS for r in regions):
    raise ValueError(f"regions must be a non-empty comma list drawn from {REGIONS}, got {regions}")

async with await connect() as conn:
    await create_claims_table(conn)
    await create_regions_table(conn)
    created = await create_claim(conn, {
        "claim_id": claim_id, "product": dbutils.widgets.get("product"),
        "medium": dbutils.widgets.get("medium"), "claim_text": claim_text, "regions": regions,
    })
if not created:
    raise RuntimeError(f"claim id {claim_id!r} already exists. Change the claim_id box and run this cell again.")
print("submitted:", claim_id, "|", regions)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Run the parent graph
# MAGIC Shared checks run once (what the claim asserts, and facts about the evidence), then each
# MAGIC region is started. The parent never waits for reviewers.

# COMMAND ----------

STARTER = start_region if dbutils.widgets.get("starter") == "in-process" else start_region_job

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    parent = build_parent_graph(checkpointer, STARTER)
    t0 = time.time()
    out = await parent.ainvoke({"claim_id": claim_id}, {"configurable": {"thread_id": claim_id}})
    print(f"parent finished in {time.time() - t0:.0f}s")
    print("dispatched:", out["dispatched"])

async with await connect() as conn:
    print("claim status:", (await get_claim(conn, claim_id))["status"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Wait until every region is paused, then show what each reviewer would see

# COMMAND ----------

async def wait_until_paused(child, timeout_s=900):
    deadline = time.time() + timeout_s
    while True:
        states = {r: await child.aget_state({"configurable": {"thread_id": f"{claim_id}:{r}"}}) for r in regions}
        for r, s in states.items():
            errors = [t.error for t in s.tasks if t.error]
            if errors:
                raise RuntimeError(f"{r} failed: {errors}")
        pending = [r for r, s in states.items() if s.next != ("human_review",)]
        if not pending:
            return states
        if time.time() > deadline:
            raise RuntimeError(f"regions not paused after {timeout_s}s: {pending}")
        await asyncio.sleep(15)

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    child = build_child_graph(checkpointer)
    states = await wait_until_paused(child)

recommendations = {}
for region, s in states.items():
    v = s.values
    recommendations[region] = v["recommendation"]
    print(f"\n===== {region} =====")
    print("recommendation:", v["recommendation"], "| risk:", v["risk"])
    print("rationale:", v["rationale"])
    for r in v["regulatory"]["rules"]:
        print(f"   {r['rule']:7} {r['status']:17} {r['reason']}")
    uncovered = [a["text"] for a in v["regulatory"]["assertions"] if not a["covered_by"]]
    if uncovered:
        print("   no rule covers:", uncovered)
    if v.get("similar_error"):
        print("   lesson lookup failed:", v["similar_error"])
    for h in v.get("similar", []):
        print(f"   similar lesson ({h['score']:.2f}) {h['claim_id']}: system said {h['system_recommendation']}, "
              f"reviewer said {h['reviewer_decision']}: {h['reason']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. The reviewers answer
# MAGIC Edit the answers if you like. The contract the graph expects (and that the App must enforce):
# MAGIC `decision` is exactly `approve` or `reject`, and `reason` is required when the decision differs from
# MAGIC the system's recommendation, because overrides are saved as lessons.
# MAGIC
# MAGIC **Run this cell once.** Each answer is final.

# COMMAND ----------

ANSWERS = {
    "UK": {"decision": "reject"},
    "US": {"decision": "approve", "reason": "US Rule 3 sets no timing requirement; two independent trials support the effect."},
    "IN": {"decision": "approve", "reason": "Study 2 has a 60-participant India cohort; timing is not relevant to India Rule 2."},
}

# Enforce the contract for every region BEFORE resuming any of them.
for region in regions:
    a = ANSWERS.get(region)
    if a is None:
        raise ValueError(f"no answer given for {region}")
    if a.get("decision") not in ("approve", "reject"):
        raise ValueError(f"{region}: decision must be 'approve' or 'reject', got {a.get('decision')!r}")
    if a["decision"] != recommendations[region] and not str(a.get("reason", "")).strip():
        raise ValueError(f"{region}: a reason is required when overriding the system's {recommendations[region]!r}")

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    child = build_child_graph(checkpointer)
    for region in regions:
        r = await child.ainvoke(Command(resume=ANSWERS[region]), {"configurable": {"thread_id": f"{claim_id}:{region}"}})
        print(f"{region}: override={r['override']} | claim_closed={r['claim_closed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. What the system recorded

# COMMAND ----------

async with await connect() as conn:
    print("claim status:", (await get_claim(conn, claim_id))["status"])
    print("tracker:", await list_regions(conn, claim_id))

async with make_store() as store:
    for region in regions:
        item = await store.aget(lesson_namespace(region), claim_id)
        print(f"{region} lesson saved:", item is not None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: run the regions as Databricks Jobs
# MAGIC In production, each region runs as its own Job run, so the parent hands them off and finishes
# MAGIC at once. To try it:
# MAGIC 1. Create a Job whose task is the notebook `job_run_region`, with three **job parameters**
# MAGIC    named `claim_id`, `region` and `endpoint` (see the README), and note the Job's id.
# MAGIC 2. Restart this notebook, set `starter` to `job`, put the Job's id in `region_job_id`, and use a
# MAGIC    new `claim_id`.
# MAGIC 3. Run the cells in order. Section 3 waits (several minutes) for the Job runs to reach the pause.
