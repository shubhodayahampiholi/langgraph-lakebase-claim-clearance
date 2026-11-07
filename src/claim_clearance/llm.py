"""One place that talks to the chat model, and refuses anything that isn't valid JSON."""
import json
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from claim_clearance.config import MODEL_ENDPOINT


def ask_json(system, user, required_keys):
    """Send one prompt, return the reply parsed as a dict. Raises on anything unusable."""
    w = WorkspaceClient()
    response = w.serving_endpoints.query(
        name=MODEL_ENDPOINT,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
            ChatMessage(role=ChatMessageRole.USER, content=user),
        ],
        temperature=0,
        max_tokens=1500,
    )
    text = response.choices[0].message.content

    # Models often wrap JSON in a markdown fence; remove it if present.
    text = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", text.strip())

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"model did not return valid JSON: {e}. Reply began: {text[:200]!r}")
    if not isinstance(data, dict):
        raise ValueError(f"model returned {type(data).__name__}, expected a JSON object")

    missing = [k for k in required_keys if k not in data]
    if missing:
        raise ValueError(f"model reply is missing keys {missing}. Reply began: {text[:200]!r}")
    return data
