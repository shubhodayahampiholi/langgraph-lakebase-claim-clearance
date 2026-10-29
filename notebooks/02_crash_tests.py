# Databricks notebook source
# MAGIC %md
# MAGIC ### Crash tests
# MAGIC We make a step fail on purpose, continue the same thread, and check what the retry does.
# MAGIC Fill in the `endpoint` box at the top, then run the cells in order. Don't re-run a test cell.

# COMMAND ----------

# MAGIC %pip install -U "databricks-langchain[memory]" langgraph

# COMMAND ----------

# MAGIC %pip uninstall -y psycopg-binary

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys

dbutils.widgets.text("endpoint", "")
endpoint = dbutils.widgets.get("endpoint")
if not endpoint:
    raise ValueError("Fill in the endpoint box: projects/<project>/branches/<branch>/endpoints/<endpoint>")
os.environ["CLAIM_CLEARANCE_ENDPOINT"] = endpoint  # must be set before importing claim_clearance

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(nb_path))
sys.path.insert(0, f"/Workspace{repo_root}/src")

from databricks_langchain import AsyncCheckpointSaver

from claim_clearance.config import ENDPOINT
from claim_clearance.db import connect
from claim_clearance.claims import create_claims_table, create_claim, get_claim
from claim_clearance.tracker import create_regions_table, list_regions
from claim_clearance.child_graph import build_child_graph
from claim_clearance.parent_graph import build_parent_graph
from claim_clearance.runner import start_region

print("ready | endpoint:", ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: dispatch crashes after starting UK, before starting US

# COMMAND ----------

import inspect
import claim_clearance.parent_graph as pg

CLAIM_ID = "CLM-1500"
cfg = {"configurable": {"thread_id": CLAIM_ID}}

# 1. Count how often the shared steps run (wrapped before the graph is built).
calls = {"load_claim": 0, "intent_check": 0, "evidence_check": 0}

def counted(name, fn):
    if inspect.iscoroutinefunction(fn):
        async def wrapper(state):
            calls[name] += 1
            return await fn(state)
    else:
        def wrapper(state):
            calls[name] += 1
            return fn(state)
    return wrapper

for name in calls:
    setattr(pg, name, counted(name, getattr(pg, name)))

# 2. A region starter that fails the first time it is asked to start US.
us_attempts = {"count": 0}

async def flaky_start_region(claim_id, region):
    if region == "US":
        us_attempts["count"] += 1
        if us_attempts["count"] == 1:
            raise RuntimeError("simulated crash while starting US")
    return await start_region(claim_id, region)

# 3. Print what the database and the checkpoints say.
async def report(checkpointer, parent):
    child = build_child_graph(checkpointer)
    p = await parent.aget_state(cfg)
    print("  parent next:", p.next, "| errors:", [t.error for t in p.tasks if t.error])
    async with await connect() as conn:
        print("  claim status:", (await get_claim(conn, CLAIM_ID))["status"])
        print("  tracker:", await list_regions(conn, CLAIM_ID))
    for region in ["UK", "US"]:
        s = await child.aget_state({"configurable": {"thread_id": f"{CLAIM_ID}:{region}"}})
        print(f"  {region} thread next:", s.next)
    print("  shared steps ran:", calls)

# 4. The test.
async with await connect() as conn:
    await create_claims_table(conn)
    await create_regions_table(conn)
    await create_claim(conn, {
        "claim_id": CLAIM_ID, "product": "PainRelief Tablets", "medium": "TV",
        "claim_text": "Clinically proven to relieve pain in 15 minutes",
        "regions": ["UK", "US"],
    })

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    parent = build_parent_graph(checkpointer, flaky_start_region)

    print("RUN 1")
    try:
        await parent.ainvoke({"claim_id": CLAIM_ID}, cfg)
    except Exception as e:
        print("  crashed:", repr(e))
    print("AFTER THE CRASH")
    await report(checkpointer, parent)

    print("RUN 2 (continue the same thread)")
    out = await parent.ainvoke(None, cfg)  # None input = continue from the last checkpoint
    print("  dispatched:", out["dispatched"])
    print("AFTER THE RETRY")
    await report(checkpointer, parent)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: finalize crashes after saving the lesson, before recording the decision

# COMMAND ----------

from langgraph.types import Command
import claim_clearance.child_graph as cg
from claim_clearance.memory import make_store, lesson_namespace

CLAIM_ID_2 = "CLM-1600"
cfg_parent_2 = {"configurable": {"thread_id": CLAIM_ID_2}}
cfg_uk = {"configurable": {"thread_id": f"{CLAIM_ID_2}:UK"}}

async with await connect() as conn:
    await create_claim(conn, {
        "claim_id": CLAIM_ID_2, "product": "PainRelief Tablets", "medium": "TV",
        "claim_text": "Clinically proven to relieve pain in 15 minutes",
        "regions": ["UK"],
    })

# A version of decide_region that fails on its first call only.
real_decide = cg.decide_region
decide_attempts = {"count": 0}

async def flaky_decide(conn, claim_id, region, decision):
    decide_attempts["count"] += 1
    if decide_attempts["count"] == 1:
        raise RuntimeError("simulated crash while recording the decision")
    return await real_decide(conn, claim_id, region, decision)

async def report2(child):
    s = await child.aget_state(cfg_uk)
    print("  UK thread next:", s.next, "| errors:", [t.error for t in s.tasks if t.error])
    async with await connect() as conn:
        print("  claim status:", (await get_claim(conn, CLAIM_ID_2))["status"])
        print("  tracker:", await list_regions(conn, CLAIM_ID_2))
    async with make_store() as store:
        item = await store.aget(lesson_namespace("UK"), CLAIM_ID_2)
        print("  lesson saved:", item is not None)

async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
    parent = build_parent_graph(checkpointer, start_region)
    await parent.ainvoke({"claim_id": CLAIM_ID_2}, cfg_parent_2)  # UK pauses at human_review

    cg.decide_region = flaky_decide  # from now on, finalize's first attempt fails
    try:
        child = build_child_graph(checkpointer)

        print("RUN 1 (reviewer overrides)")
        try:
            await child.ainvoke(
                Command(resume={"decision": "reject", "reason": "Evidence too thin"}), cfg_uk
            )
        except Exception as e:
            print("  crashed:", repr(e))
        print("AFTER THE CRASH")
        await report2(child)

        print("RUN 2 (continue; the reviewer is not asked again)")
        r = await child.ainvoke(None, cfg_uk)
        print("  decision kept:", r["decision"], "| claim_closed:", r["claim_closed"])
        print("AFTER THE RETRY")
        await report2(child)
    finally:
        cg.decide_region = real_decide  # always put the real function back
