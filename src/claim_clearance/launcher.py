"""Starts a region's review as a Databricks Job (fire and forget).

Same signature as runner.start_region, so the parent graph can use either one.
"""
from databricks.sdk import WorkspaceClient

from claim_clearance.config import ENDPOINT, REGION_JOB_ID


async def start_region_job(claim_id, region):
    if not REGION_JOB_ID.isdigit():
        raise ValueError(
            "REGION_JOB_ID is not set. Set CLAIM_CLEARANCE_REGION_JOB_ID to your Job's ID."
        )
    w = WorkspaceClient()
    run = w.jobs.run_now(
        job_id=int(REGION_JOB_ID),
        job_parameters={"claim_id": claim_id, "region": region, "endpoint": ENDPOINT},
        # Same token -> Databricks launches exactly one run, even if this is retried.
        idempotency_token=f"{claim_id}:{region}",
    )
    return f"job run {run.run_id}"
