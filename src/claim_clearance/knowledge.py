"""Inputs for the knowledge check: a region's rules, and the global product evidence.

A review needs EVERY rule of its region, because a missed rule means a wrong clearance.
So this does not rank by relevance. The search index is configured to return every chunk
of a scope, in document order, and the similarity score is ignored. The index is the
standard, governed component, and similarity ranking becomes the right tool only when a
corpus is too large to hand to a model whole.

The region must come from validated state (the claim's selected regions), never from
model output.
"""
import json

from databricks.sdk import WorkspaceClient

from claim_clearance.config import SEARCH_INDEX

REGIONS = ("UK", "US", "IN")
COLUMNS = ["chunk_key", "doc_id", "section_index", "section_header", "chunk_text"]
CAP = 20  # larger than any scope holds; hitting it would mean results were cut off


def _fetch_scope(scope, query_text):
    w = WorkspaceClient()
    res = w.vector_search_indexes.query_index(
        index_name=SEARCH_INDEX,
        columns=COLUMNS,
        query_text=query_text,  # the API requires it; it cannot change which rows match
        filters_json=json.dumps({"scope": scope}),  # built with json, never string formatting
        num_results=CAP,
    )
    rows = res.result.data_array or []
    if len(rows) >= CAP:
        raise RuntimeError(f"scope {scope!r} returned {len(rows)} chunks: results may be cut off")

    # Each row ends with the similarity score. zip stops at COLUMNS, so the score is dropped.
    chunks = [dict(zip(COLUMNS, row)) for row in rows]
    chunks.sort(key=lambda c: (c["doc_id"], int(c["section_index"])))  # document order
    return chunks


def get_region_rules(region, claim_text):
    """Every rule chunk for one region, in document order."""
    if region not in REGIONS:
        raise ValueError(f"unknown region {region!r}; expected one of {REGIONS}")
    return _fetch_scope(region, claim_text)


def get_global_evidence(claim_text):
    """Every global evidence chunk, in document order."""
    return _fetch_scope("global", claim_text)
