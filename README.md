# langgraph-lakebase-claim-clearance

A reference implementation of a **long-running agent with human review**, built with LangGraph and Databricks Lakebase. A brand manager submits an advertising claim for several regions. The system reviews it once for shared facts, then separately per region, then **waits as long as each regional reviewer takes**. The claim closes when the last region decides.

Companion article: _[link to the article]_

> **All data is synthetic.** The product, the studies and the regional "standards" are invented for this example. Nothing here describes any real company, regulator or production system. Databricks Free Edition is for non-commercial use.

## What it demonstrates

| | Checkpointer | Store (long-term memory) |
|---|---|---|
| Scope | One claim in one region (a thread) | All claims, by region |
| Answers | "Where was I, and what did I know?" | "What have reviewers overridden before?" |
| Used for | Pausing for a human, resuming after a crash, audit | Showing the reviewer similar past overrides |
| Written by | LangGraph, after every step | `finalize`, on a reviewer override |

```
PARENT (thread = claim_id)                      finishes in seconds, never waits for a reviewer
  load_claim ─┬─> intent_check ───┐
              └─> evidence_check ─┴─> dispatch ─> end
                                        │  registers regions, starts one review per region
        ┌───────────────────────────────┼───────────────────────────────┐
CHILD (thread = claim_id:UK)    (claim_id:US)                    (claim_id:IN)
  load_context ─┬─> regulatory_check ─┐
                └─> similar_claims ───┴─> risk_score ─> recommend ─> human_review ─> finalize
                                                                      (pauses, days if needed)
```

- The system **only recommends**. A human approves, rejects or overrides in every region.
- Regions can disagree, and that is a normal outcome. Overrides are saved as lessons.
- A small `claim_regions` table lets the **last region to decide close the claim**, under a lock.
- Every step that has a side effect is safe to repeat, so a crashed step can be retried.

## Status: what is verified

| Piece | State |
|---|---|
| Checkpointing, tracker, parent and child graphs, memory | Run in the author's workspace |
| Crash tests (`05_crash_tests`) | Run in the author's workspace |
| Region start as a Databricks Job (`job_run_region`, `launcher.py`) | Run in the author's workspace |
| `00_setup` to `04_run_a_claim` | Run in the author's workspace, where the catalog, tables, endpoint and index already existed. |


Model outputs vary between runs, even at temperature 0. In testing, the same claim was judged differently by the same model on different days.

## Quick start (Databricks Free Edition)

1. **Create a Lakebase project** (Lakebase, then create a project). Note the endpoint path `projects/<project>/branches/<branch>/endpoints/<endpoint>`. These are resource *names*. The UI also shows UIDs (`br-...`, `ep-...`), which are not the same thing.
2. **Add this repo as a Git folder** under Workspace. The Job notebook finds the repo’s code from its own workspace path, so we ran the Job task with a Workspace source (not tested with a Git-provider source).
3. **Run the notebooks in order**, each on Serverless. In each one, the first run stops at a widget error until you fill in the boxes at the top.

| Notebook | Does |
|---|---|
| `00_setup` | Installs packages, checks the Lakebase endpoint, creates the catalog, volume and tables |
| `01_build_corpus` | Turns `data/corpus/*.md` into PDFs in a volume |
| `02_parse_and_chunk` | Parses the PDFs with `ai_parse_document`, then chunks one section per chunk |
| `03_create_search_index` | Creates the search endpoint and index (10 to 20 minutes) |
| `04_run_a_claim` | Submits a claim and plays the reviewers' answers |
| `05_crash_tests` | Breaks `dispatch` and `finalize` on purpose and shows the retry |
| `job_run_region` | What a Databricks Job runs for one region (optional) |

Each setup cell begins with the same three package cells. Keep them, in order:

```python
%pip install -U "databricks-langchain[memory]" langgraph
%pip uninstall -y psycopg-binary      # see Troubleshooting: FIPS crash
dbutils.library.restartPython()
```

## Configuration

Set these as environment variables **before importing `claim_clearance`**. They are read once, at import.

| Variable | Meaning | Default |
|---|---|---|
| `CLAIM_CLEARANCE_ENDPOINT` | Lakebase endpoint path | placeholder |
| `CLAIM_CLEARANCE_SEARCH_INDEX` | `<catalog>.<schema>.chunks_index` | placeholder |
| `CLAIM_CLEARANCE_REGION_JOB_ID` | Job id, for the optional Job path | placeholder |
| `CLAIM_CLEARANCE_MODEL` | Chat model endpoint | `databricks-llama-4-maverick` |

## Repo layout

```
src/claim_clearance/
  config.py        settings
  db.py            Lakebase SQL connection (short-lived token, no stored password)
  claims.py        the claims table and its status (forward only)
  tracker.py       which regions are still waiting; the last region closes the claim
  memory.py        long-term lessons from overrides, one namespace per region
  knowledge.py     a region's rules and the global evidence, fetched whole
  llm.py           one place that calls the model and refuses anything but valid JSON
  intent.py        what a claim asserts, plus an injection flag
  evidence.py      neutral facts about the evidence, checked against the source text
  judge.py         judges each of a region's rules
  parent_graph.py  shared checks, then dispatch
  child_graph.py   one region's review, ending in the human pause
  runner.py        start one region in-process
  launcher.py      start one region as a Databricks Job
data/corpus/       the synthetic documents
notebooks/         see the table above
```

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Notebook dies with `exit code 134` and `FATAL FIPS SELFTEST FAILURE` | The binary build of psycopg bundles its own OpenSSL, which fails on Databricks serverless. Run `%pip uninstall -y psycopg-binary`, then restart Python |
| `ModuleNotFoundError: No module named 'claim_clearance'` | A restart wipes the import path and environment. Re-run the setup cell, from its first line |
| `ImportError: cannot import name ...` from `config` | You edited or pulled `config.py` after it was imported. Restart Python |
| `The endpoint has been disabled. Enable it using the API and retry.` | After a long idle period the Lakebase endpoint can be left disabled. It still shows as IDLE. `00_setup` can clear the flag if you ask it to |
| A widget box doesn't appear | Boxes appear after the cell that defines them has run once |
| `claim id ... already exists` | A finished thread can't be reused. Change the claim id |
| Paused review shows `finalize` as next, with `KeyError` | A node returned nothing, or a malformed answer was saved. Saved state stays saved, so use a new claim id |

## Not solved (production hardening)

- **Nothing resumes a stuck thread.** A crashed parent or region stays at its last checkpoint until something continues it. `runner.start_region` skips any thread that already has state, which also hides a thread that is stuck.
- **A failed Job run can't be relaunched** with the same idempotency token, because Databricks returns the old run.
- **The judge is not reliable.** Python checks that every rule and assertion is covered once, not that a verdict is correct. A human reviews every region for that reason, and a real system needs an evaluation set. In testing, one region's rule was misjudged even after the standard was made explicit.
- **The injection flag is a filter, not a boundary.** An obvious injected instruction was caught in testing, and the injected sentence also came back as an assertion.
- **Checkpoints hold claim text and findings.** Treat them like the data the agent uses: access control, and a retention rule for closed claims.
- **Ingestion is manual.** Changing a document means re-parsing, re-chunking and syncing the index by hand. A real system would ingest immutable, versioned files incrementally and propagate deletions.
- **No App yet.** The reviewer's answers are played from a notebook, and the App would validate them at its door.
- **Jobs run as your identity.** Production would use a service principal with narrow access.

## License

MIT
