"""Tracks which regions of a claim are still awaiting review.

One row per (claim, region). A claim is closed when no region is awaiting.
"""


async def create_regions_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS claim_regions (
            claim_id   text NOT NULL,
            region     text NOT NULL,
            status     text NOT NULL DEFAULT 'awaiting_review',
            decision   text,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (claim_id, region)
        )
    """)


async def register_claim(conn, claim_id, regions):
    # ON CONFLICT DO NOTHING makes this safe to repeat if dispatch re-runs.
    for region in regions:
        await conn.execute(
            "INSERT INTO claim_regions (claim_id, region) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING",
            (claim_id, region),
        )


async def list_regions(conn, claim_id):
    cur = await conn.execute(
        "SELECT region, status, decision FROM claim_regions "
        "WHERE claim_id = %s ORDER BY region",
        (claim_id,),
    )
    return await cur.fetchall()


async def decide_region(conn, claim_id, region, decision):
    """Record a region's decision. Returns how many regions are still awaiting.

    The caller that gets 0 back is the one that closes the claim.
    """
    async with conn.transaction():
        # Lock all rows of this claim so two reviewers finishing at the same moment
        # take turns. Without this, each could see the other as still waiting and
        # neither would close the claim.
        await conn.execute(
            "SELECT 1 FROM claim_regions WHERE claim_id = %s FOR UPDATE", (claim_id,)
        )
        # Only updates a region still awaiting, so a repeated call changes nothing.
        await conn.execute(
            "UPDATE claim_regions SET status = 'decided', decision = %s, "
            "updated_at = now() "
            "WHERE claim_id = %s AND region = %s AND status = 'awaiting_review'",
            (decision, claim_id, region),
        )
        cur = await conn.execute(
            "SELECT count(*) FROM claim_regions "
            "WHERE claim_id = %s AND status = 'awaiting_review'",
            (claim_id,),
        )
        (remaining,) = await cur.fetchone()
    return remaining
