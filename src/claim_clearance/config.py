"""All settings a reader may need to change live in this one file."""

# Your Lakebase compute endpoint, as projects/<project>/branches/<branch>/endpoints/<endpoint>.
ENDPOINT = "projects/<your-project-name>/branches/<branch-name>/endpoints/<endpoint-name>"

# Embedding model used to match similar claims, and its vector size.
EMBEDDING_ENDPOINT = "databricks-gte-large-en"
EMBEDDING_DIMS = 1024
