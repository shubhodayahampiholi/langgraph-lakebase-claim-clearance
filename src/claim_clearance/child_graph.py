"""Child graph: the review of one claim in one region.

Runs on its own thread (claim_id:region), so each region's reviewer resumes
independently. Input is only claim_id and region; everything else is read from
the claims table.

Real: load_context, regulatory_check (a model judges each rule), similar_claims (past
overrides, shown to the reviewer only), risk_score and recommend (plain Python over the
verdicts), human_review, finalize.
"""
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt

from claim_clearance.claims import get_claim, set_status
from claim_clearance.db import connect
from claim_clearance.judge import judge_region
from claim_clearance.knowledge import get_region_rules
from claim_clearance.memory import make_store, save_lesson, find_similar_lessons
from claim_clearance.tracker import decide_region

LESSON_FIELDS = ["claim_id", "claim_text", "system_recommendation", "reviewer_decision", "reason", "score"]


class ChildState(TypedDict, total=False):
    claim_id: str
    region: str
    product: str
    medium: str
    claim_text: str
    global_findings: dict
    regulatory: dict
    similar: list
    similar_error: str
    risk: str
    recommendation: str
    rationale: str
    decision: dict
    override: bool
    claim_closed: bool


async def load_context(state):
    async with await connect() as conn:
        row = await get_claim(conn, state["claim_id"])
    if row is None:
        raise ValueError(f"unknown claim_id: {state['claim_id']}")
    # The region arrives from outside (a Job parameter), so check it against the claim.
    if state["region"] not in row["regions"]:
        raise ValueError(
            f"region {state['region']!r} was not selected for claim {state['claim_id']}: {row['regions']}"
        )
    return {k: row[k] for k in ["product", "medium", "claim_text", "global_findings"]}


def regulatory_check(state):
    findings = state["global_findings"]
    if not findings:
        raise ValueError("this claim has no global findings yet; the parent graph must run first")
    chunks = get_region_rules(state["region"], state["claim_text"])
    verdict = judge_region(state["region"], chunks, findings["intent"], findings["evidence"])
    return {"regulatory": verdict}


async def similar_claims(state):
    """Past overrides in this region that look like this claim. Shown to the reviewer only:
    risk_score and recommend never read them."""
    try:
        async with make_store() as store:
            hits = await find_similar_lessons(store, state["region"], state["claim_text"], limit=4)
        # Leave out this claim's own lesson (it could exist if the node re-runs), keep three.
        hits = [h for h in hits if h["claim_id"] != state["claim_id"]][:3]
        return {"similar": [{k: h.get(k) for k in LESSON_FIELDS} for h in hits]}
    except Exception as e:
        # Context must not block a review, but the failure must be visible to the reviewer.
        return {"similar": [], "similar_error": f"{type(e).__name__}: {str(e)[:200]}"}


def risk_score(state):
    verdict = state["regulatory"]
    statuses = [r["status"] for r in verdict["rules"]]
    uncovered = [a for a in verdict["assertions"] if not a["covered_by"]]
    injection = state["global_findings"]["intent"]["injection_suspected"]

    if "not_met" in statuses or injection:
        risk = "high"
    elif "cannot_determine" in statuses or uncovered:
        risk = "medium"
    else:
        risk = "low"
    return {"risk": risk}


def recommend(state):
    verdict = state["regulatory"]
    not_met = [r for r in verdict["rules"] if r["status"] == "not_met"]
    unknown = [r for r in verdict["rules"] if r["status"] == "cannot_determine"]
    uncovered = [a["text"] for a in verdict["assertions"] if not a["covered_by"]]

    # The system never guesses: anything unmet or undeterminable blocks an approval.
    recommendation = "reject" if (not_met or unknown) else "approve"

    parts = [f"{r['rule']} not met: {r['reason']}" for r in not_met]
    parts += [f"{r['rule']} cannot be determined: {r['reason']}" for r in unknown]
    if uncovered:
        parts.append("Not covered by any rule in this standard: " + "; ".join(repr(t) for t in uncovered))
    if state["global_findings"]["intent"]["injection_suspected"]:
        parts.append("The claim text was flagged as containing an instruction aimed at an AI system.")
    if not parts:
        parts.append("Every applicable rule is met.")
    return {"recommendation": recommendation, "rationale": " ".join(parts)}


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
    decision = interrupt({
        "claim_id": state["claim_id"],
        "region": state["region"],
        "claim_text": state["claim_text"],
        "global_findings": state["global_findings"],
        "regulatory": state["regulatory"],
        "recommendation": state["recommendation"],
        "rationale": state["rationale"],
        "risk": state["risk"],
        "similar_lessons": state.get("similar", []),
        "similar_error": state.get("similar_error"),
    })
    return {"decision": decision}


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
