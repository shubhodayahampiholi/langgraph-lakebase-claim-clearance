"""Parent graph: the shared work for a claim, then hand-off to the regions.

Runs on thread = claim_id. It ends right after dispatch and never waits for reviewers.
Both shared checks are real: intent (what the claim asserts) and evidence (facts only).
"""
from typing import TypedDict

from langgraph.graph import StateGraph, START, END

from claim_clearance.claims import get_claim, save_global_findings, set_status
from claim_clearance.db import connect
from claim_clearance.evidence import extract_evidence_facts
from claim_clearance.intent import classify_intent
from claim_clearance.knowledge import get_global_evidence
from claim_clearance.tracker import register_claim


class ParentState(TypedDict, total=False):
    claim_id: str
    claim: dict
    intent: dict
    evidence: dict
    dispatched: dict


async def load_claim(state):
    async with await connect() as conn:
        row = await get_claim(conn, state["claim_id"])
    if row is None:
        raise ValueError(f"unknown claim_id: {state['claim_id']}")
    keep = ["product", "medium", "claim_text", "regions"]  # lean state
    return {"claim": {k: row[k] for k in keep}}


def intent_check(state):
    # Sees only the claim text. Nothing is blocked: a flagged claim is still reviewed.
    return {"intent": classify_intent(state["claim"]["claim_text"])}


def evidence_check(state):
    # The claim text is only the search call's required query text. It cannot change
    # which chunks come back, and the extraction below never sees it.
    chunks = get_global_evidence(state["claim"]["claim_text"])
    return {"evidence": extract_evidence_facts(chunks)}


def make_dispatch(start_region):
    """start_region(claim_id, region) is passed in, so how a region gets started
    (in-process now, a Databricks Job later) can change without touching the graph."""

    async def dispatch(state):
        claim_id = state["claim_id"]
        regions = state["claim"]["regions"]

        # Every step below is safe to repeat if this node re-runs.
        async with await connect() as conn:
            await save_global_findings(
                conn, claim_id, {"intent": state["intent"], "evidence": state["evidence"]}
            )
            await register_claim(conn, claim_id, regions)

        outcome = {}
        for region in regions:
            outcome[region] = await start_region(claim_id, region)

        # Last: "in_review" means the regions were launched.
        async with await connect() as conn:
            await set_status(conn, claim_id, "in_review", ["submitted"])
        return {"dispatched": outcome}

    return dispatch


def build_parent_graph(checkpointer, start_region):
    g = StateGraph(ParentState)
    g.add_node("load_claim", load_claim)
    g.add_node("intent_check", intent_check)
    g.add_node("evidence_check", evidence_check)
    g.add_node("dispatch", make_dispatch(start_region))
    g.add_edge(START, "load_claim")
    g.add_edge("load_claim", "intent_check")
    g.add_edge("load_claim", "evidence_check")
    g.add_edge("intent_check", "dispatch")
    g.add_edge("evidence_check", "dispatch")
    g.add_edge("dispatch", END)
    return g.compile(checkpointer=checkpointer)
