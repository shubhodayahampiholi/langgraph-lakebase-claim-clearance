"""Child graph: the review of one claim in one region.

Runs on its own thread (claim_id:region), so each region's reviewer resumes
independently. Input is only claim_id and region; everything else is read from
the claims table. Check nodes are placeholders for now.
"""
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt

from claim_clearance.claims import get_claim, set_status
from claim_clearance.db import connect
from claim_clearance.memory import make_store, save_lesson
from claim_clearance.tracker import decide_region


class ChildState(TypedDict, total=False):
    claim_id: str
    region: str
    product: str
    medium: str
    claim_text: str
    global_findings: dict
    regulatory: str
    similar: list
    risk: str
    recommendation: str
    decision: dict
    override: bool
    claim_closed: bool


async def load_context(state):
    async with await connect() as conn:
        row = await get_claim(conn, state["claim_id"])
    if row is None:
        raise ValueError(f"unknown claim_id: {state['claim_id']}")
    return {k: row[k] for k in ["product", "medium", "claim_text", "global_findings"]}


def regulatory_check(state):
    return {"regulatory": "placeholder: no rule violations found"}


def similar_claims(state):
    return {"similar": []}


def risk_score(state):
    return {"risk": "medium"}


def recommend(state):
    return {"recommendation": "approve"}


def human_review(state):
    """Pauses until a reviewer answers, then stores the answer in state.

    The graph expects the resume value to be a dict:
        {"decision": "approve" | "reject", "reason": "<text>"}
    - decision is required and must be exactly "approve" or "reject".
    - reason is required only when decision differs from the system's recommendation
      (an override), because overrides are saved as lessons.

    The graph does NOT validate this. The App is the single door reviewers answer
    through and must enforce it. A malformed answer that gets past the App is saved
    in the checkpoint and will fail finalize on every retry (the claim gets stuck).
    """
    # Nothing before interrupt(): this node re-runs from the top on resume.


async def finalize(state):
    # All side effects happen here, after the pause, and every one is safe to repeat.
    decision = state["decision"]
    override = decision["decision"] != state["recommendation"]

    # 1. Save the lesson first (keyed by claim_id, so a re-run replaces it).
    if override:
        async with make_store() as store:
            await save_lesson(store, {
                "claim_id": state["claim_id"], "region": state["region"],
                "product": state["product"], "medium": state["medium"],
                "claim_text": state["claim_text"],
                "system_recommendation": state["recommendation"],
                "reviewer_decision": decision["decision"],
                "reason": decision.get("reason", ""),
            })

    # 2. Record the decision, then close the claim if this was the last region.
    async with await connect() as conn:
        remaining = await decide_region(
            conn, state["claim_id"], state["region"], decision["decision"]
        )
        if remaining == 0:
            # Allowed from "submitted" too, so a very fast reviewer can't leave the
            # claim stuck open. A repeat or late call is a harmless no-op.
            await set_status(conn, state["claim_id"], "closed", ["submitted", "in_review"])

    return {"override": override, "claim_closed": remaining == 0}


def build_child_graph(checkpointer):
    g = StateGraph(ChildState)
    for name, fn in [
        ("load_context", load_context),
        ("regulatory_check", regulatory_check), ("similar_claims", similar_claims),
        ("risk_score", risk_score), ("recommend", recommend),
        ("human_review", human_review), ("finalize", finalize),
    ]:
        g.add_node(name, fn)
    g.add_edge(START, "load_context")
    g.add_edge("load_context", "regulatory_check")
    g.add_edge("load_context", "similar_claims")
    g.add_edge("regulatory_check", "risk_score")
    g.add_edge("similar_claims", "risk_score")
    g.add_edge("risk_score", "recommend")
    g.add_edge("recommend", "human_review")
    g.add_edge("human_review", "finalize")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer)
