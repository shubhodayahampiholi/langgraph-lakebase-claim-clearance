"""Child graph: the review of one claim in one region.

Runs on its own thread (claim_id:region), so each region's reviewer resumes
independently. Nodes are placeholders for now; real checks replace them later.
"""
from typing import TypedDict

from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt


class ChildState(TypedDict, total=False):
    claim_id: str
    region: str
    product: str
    medium: str
    claim_text: str
    regulatory: str
    similar: list
    risk: str
    recommendation: str
    decision: dict
    override: bool


def regulatory_check(state):
    return {"regulatory": "placeholder: no rule violations found"}


def similar_claims(state):
    return {"similar": []}


def risk_score(state):
    return {"risk": "medium"}


def recommend(state):
    return {"recommendation": "approve"}


def human_review(state):
    # Nothing before interrupt(): this node re-runs from the top on resume.
    decision = interrupt({
        "claim_id": state["claim_id"],
        "region": state["region"],
        "claim_text": state["claim_text"],
        "recommendation": state["recommendation"],
        "risk": state["risk"],
    })
    return {"decision": decision}


def finalize(state):
    # Side effects (tracker, memory) are wired here next. They belong AFTER the pause.
    return {"override": state["decision"]["decision"] != state["recommendation"]}


def build_child_graph(checkpointer):
    g = StateGraph(ChildState)
    for name, fn in [
        ("regulatory_check", regulatory_check), ("similar_claims", similar_claims),
        ("risk_score", risk_score), ("recommend", recommend),
        ("human_review", human_review), ("finalize", finalize),
    ]:
        g.add_node(name, fn)
    g.add_edge(START, "regulatory_check")
    g.add_edge(START, "similar_claims")
    g.add_edge("regulatory_check", "risk_score")
    g.add_edge("similar_claims", "risk_score")
    g.add_edge("risk_score", "recommend")
    g.add_edge("recommend", "human_review")
    g.add_edge("human_review", "finalize")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer)
