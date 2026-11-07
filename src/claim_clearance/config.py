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

# Full name of the AI Search index over the corpus chunks (see notebooks/04_parse_and_chunk.py).
# Set it in your notebook before importing claim_clearance:
#     os.environ["CLAIM_CLEARANCE_SEARCH_INDEX"] = "my_catalog.my_schema.chunks_index"
SEARCH_INDEX = os.environ.get(
    "CLAIM_CLEARANCE_SEARCH_INDEX", "<your-catalog>.<your-schema>.chunks_index"
)

# Chat model that performs the knowledge checks. Any chat model on your workspace that follows
# JSON instructions reliably will work; swap it only when a concrete test fails.
MODEL_ENDPOINT = os.environ.get("CLAIM_CLEARANCE_MODEL", "databricks-meta-llama-3-3-70b-instruct")
