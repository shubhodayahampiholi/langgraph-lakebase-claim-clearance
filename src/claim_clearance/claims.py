"""The main claims table: the claim itself plus its overall status.

Status only moves forward: submitted -> in_review -> closed.
Child graphs read the claim from here, so jobs only need claim_id and region.
"""
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


async def create_claims_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS claims (
            claim_id        text PRIMARY KEY,
            product         text NOT NULL,
            medium          text NOT NULL,
            claim_text      text NOT NULL,
            regions         text[] NOT NULL,
            status          text NOT NULL DEFAULT 'submitted',
            global_findings jsonb,
            submitted_at    timestamptz NOT NULL DEFAULT now(),
            updated_at      timestamptz NOT NULL DEFAULT now()
        )
    """)


async def create_claim(conn, claim):
    """Returns True if created, False if this claim_id already existed (first one wins)."""
    cur = await conn.execute(
        "INSERT INTO claims (claim_id, product, medium, claim_text, regions) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (claim["claim_id"], claim["product"], claim["medium"],
         claim["claim_text"], claim["regions"]),
    )
    return cur.rowcount == 1


async def get_claim(conn, claim_id):
    cur = conn.cursor(row_factory=dict_row)
    await cur.execute("SELECT * FROM claims WHERE claim_id = %s", (claim_id,))
    return await cur.fetchone()


async def save_global_findings(conn, claim_id, findings):
    await conn.execute(
        "UPDATE claims SET global_findings = %s, updated_at = now() WHERE claim_id = %s",
        (Jsonb(findings), claim_id),
    )


async def set_status(conn, claim_id, new_status, only_from):
    """Moves status forward only. Returns False if nothing changed (already moved on).

    Callers can retry safely: a repeated or late update is a no-op, not an error.
    """
    cur = await conn.execute(
        "UPDATE claims SET status = %s, updated_at = now() "
        "WHERE claim_id = %s AND status = ANY(%s)",
        (new_status, claim_id, only_from),
    )
    return cur.rowcount == 1
