"""Long-term memory: lessons learned from reviewer overrides, kept per region.
The checkpointer remembers one claim; this store remembers across claims.
"""
from databricks_langchain import AsyncDatabricksStore

from claim_clearance.config import ENDPOINT, EMBEDDING_ENDPOINT, EMBEDDING_DIMS

REQUIRED_FIELDS = [
    "claim_id", "region", "product", "medium", "claim_text",
    "system_recommendation", "reviewer_decision", "reason",
]


def make_store():
    # Only claim_text is embedded, so matching is on the claim's wording.
    return AsyncDatabricksStore(
        autoscaling_endpoint=ENDPOINT,
        embedding_endpoint=EMBEDDING_ENDPOINT,
        embedding_dims=EMBEDDING_DIMS,
        embedding_fields=["claim_text"],
    )


def lesson_namespace(region):
    # A namespace is a key prefix, not a security boundary. Callers must pass a
    # region that came from validated state, never from LLM output.
    return ("claim_lessons", region)


async def save_lesson(store, lesson):
    missing = [f for f in REQUIRED_FIELDS if f not in lesson]
    if missing:
        raise ValueError(f"lesson is missing fields: {missing}")
    # Key = claim_id, so writing the same lesson twice replaces it (safe to retry).
    await store.aput(lesson_namespace(lesson["region"]), lesson["claim_id"], lesson)


async def find_similar_lessons(store, region, claim_text, limit=3):
    hits = await store.asearch(lesson_namespace(region), query=claim_text, limit=limit)
    return [{"score": round(h.score, 3), **h.value} for h in hits]
