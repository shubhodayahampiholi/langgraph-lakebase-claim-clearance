"""All settings a reader may need to change live in this one file."""
import os

# Your Lakebase compute endpoint, as projects/<project>/branches/<branch>/endpoints/<endpoint>.
# Easiest way to set it: in your notebook, before importing claim_clearance, run
#     os.environ["CLAIM_CLEARANCE_ENDPOINT"] = "projects/my-project/branches/production/endpoints/primary"
ENDPOINT = os.environ.get(
    "CLAIM_CLEARANCE_ENDPOINT",
    "projects/<your-project-name>/branches/<branch-name>/endpoints/<endpoint>",
)
# ID of the Databricks Job that runs one region's review (see README).
# Set it in your notebook before importing claim_clearance:
#     os.environ["CLAIM_CLEARANCE_REGION_JOB_ID"] = "123456789"
REGION_JOB_ID = os.environ.get("CLAIM_CLEARANCE_REGION_JOB_ID", "<your-job-id>")

# Embedding model used to match similar claims, and its vector size.
EMBEDDING_ENDPOINT = "databricks-gte-large-en"
EMBEDDING_DIMS = 1024
