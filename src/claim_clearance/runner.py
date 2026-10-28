"""Starting a region's review from just a claim_id and a region.

This is what a Databricks Job runs for each region. It needs no shared state with
the parent graph: it opens its own checkpointer connection and reads the claim
from the claims table.
"""
from databricks_langchain import AsyncCheckpointSaver

from claim_clearance.child_graph import build_child_graph
from claim_clearance.config import ENDPOINT


async def start_region(claim_id, region):
    """Runs the region's review until it pauses for the human. Safe to call twice."""
    config = {"configurable": {"thread_id": f"{claim_id}:{region}"}}
    async with AsyncCheckpointSaver(autoscaling_endpoint=ENDPOINT) as checkpointer:
        child = build_child_graph(checkpointer)

        # If this thread already has a checkpoint, the region was started before.
        # Starting it again could disturb a review that is already waiting.
        snap = await child.aget_state(config)
        if snap.values or snap.next:
            return "skipped (already started)"

        await child.ainvoke({"claim_id": claim_id, "region": region}, config)
        return "started"
