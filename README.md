# data-cdc-replay-pipeline

![ci](https://github.com/Supp3r-Man/data-cdc-replay-pipeline/actions/workflows/ci.yml/badge.svg)

A small **change-data-capture replay** pipeline: synthetic insert/update/delete events with log sequence numbers (LSNs) arrive duplicated, out of order and with holes, and are applied to a sqlite3 table so the end state equals the source of truth. It has an idempotent apply keyed by `(key, lsn)`, last-writer-wins by LSN (not arrival order), tombstones, crash-safe checkpoint/resume, gap detection and a reconciliation check, and it measures a **fair naive baseline** against it.

> **What is verified, and what is not.** 44 tests pass locally on Python 3.13 (stdlib only, plus pytest). They include seeded property-style tests that compare the pipeline with a trivial reference (sort by LSN, apply to a dict) over 30 random configurations, plus total-shuffle and dropped-event variants. I mutation-checked 8 key behaviours (duplicate detection, LWW, tombstone flag, rollback on crash, gap range end, tombstone purge bound, resume position, reconcile value check): all 8 were caught. **CI has not run yet**, so I do not claim it passed. **All data is synthetic** (fictional `acct-NNNN` keys, one process, one table, full-row images). The numbers do **not** show how a real Debezium/Kafka/Postgres WAL pipeline behaves: no multi-table transactions, no schema change, no partial-column updates, no concurrent writers, no real network. The divergence figures depend on the simulation parameters below (especially events per key) and are not a general claim.

## Measured results

Real output of `python -m cdc_replay demo` (1500 events, 150 keys, mean of 10 seeds). Divergence = share of keys whose final state differs from the source snapshot. "naive" is an in-arrival-order sqlite upsert/delete (correct on a clean stream, which a test checks); "naive+dedupe" adds a seen-set on `(key, lsn)`, the obvious first fix.

| dup | window | naive | naive+dedupe | this repo |
|---|---|---|---|---|
| 0% | 0 | 0.0% | 0.0% | 0.0% |
| 0% | 25 | 1.9% | 1.9% | 0.0% |
| 0% | 50 | 4.3% | 4.3% | 0.0% |
| 10% | 25 | 2.3% | 2.3% | 0.0% |
| 20% | 50 | 4.1% | 4.1% | 0.0% |
| 30% | 0 | 0.0% | 0.0% | 0.0% |
| 30% | 25 | 2.0% | 2.0% | 0.0% |
| 30% | 50 | 4.8% | 4.6% | 0.0% |

(The demo prints all 20 combinations of 0/10/20/30% duplicates and windows 0/5/10/25/50.) Duplicates alone barely hurt a naive upsert, because a duplicate only does damage when it is redelivered after a newer write; reordering is what breaks it, and de-duplication does not fix that.

How hard the baseline is hit depends on how often one key is rewritten (dup 20%, window 25, same 1500 events):

| keys | events/key | naive | naive+dedupe | this repo |
|---|---|---|---|---|
| 10 | 150 | 39.0% | 38.0% | 0.0% |
| 30 | 50 | 9.3% | 9.0% | 0.0% |
| 150 | 10 | 2.3% | 2.3% | 0.0% |
| 600 | 2 | 0.6% | 0.7% | 0.0% |

**Where this repo still fails: lost events.** A replay cannot apply what never arrived (dup 20%, window 25, bursts of 1-3 consecutive LSNs lost):

| lost | LSNs lost (mean) | gap ranges | this repo diverges | after backfill | naive diverges |
|---|---|---|---|---|---|
| 1% | 26.6 | 13.4 | 2.1% | 0.0% | 3.9% |
| 3% | 94.3 | 46.4 | 7.3% | 0.0% | 9.4% |
| 10% | 270.3 | 122.9 | 18.9% | 0.0% | 20.9% |

In all 40 simulated runs gap detection found exactly the lost LSNs when the expected high LSN was given, and every divergent key had a lost event (`unexplained` = 0). Re-reading the detected ranges from the source (backfill) restores an exact match. Without a known high-water mark a lost *tail* is invisible (a test shows it happens; it did not occur in the 10 demo seeds). Crash at stream offset 777 then resume gave a table identical to the clean run, tombstones included.

## Run

```
pip install pytest          # only for the tests; the package is stdlib only
python -m pytest            # 44 tests, about 4 s
python -m cdc_replay demo   # prints the tables above (about 6 s)
python -m cdc_replay demo --seeds 3
```

## Design

- `target(key, value, lsn, deleted)`: one row per key; a delete is a **tombstone** row carrying its LSN, so a late insert with a lower LSN loses.
- `applied(key, lsn)`: idempotency ledger. `INSERT OR IGNORE` decides duplicate vs new; stale events are recorded too, so they count as received for gap detection.
- Apply rule: new `(key, lsn)` and `lsn > row.lsn` then upsert; otherwise skip. Order of arrival cannot matter.
- `replay` commits each batch **together with its checkpoint**; a crash rolls the whole batch back and resume re-reads it. Replaying from offset 0 is also safe.
- `find_gaps(upto=...)`, `low_watermark`, `purge_tombstones` (only below the watermark, so nothing older can still arrive), `reconcile` (missing / extra / mismatched), `backfill`.

## Layout

```
cdc_replay/events.py   Event, generator, reference state, fault injector (dup, reorder, drop)
cdc_replay/applier.py  schema, apply_event, replay, gaps, purge, reconcile, naive baselines
cdc_replay/sim.py      seeded simulation and table formatting
tests/                 events, apply/checkpoint/gaps, property-style vs reference, simulation + CLI
STEPS.md               how it was built and how to run it
```

## Known gaps

- Events carry **full row images**. Column-level partial updates would need ordered apply or per-column LSNs; not handled.
- Gaps are **detected and reported, not repaired automatically**; `backfill` is given events by the caller. The simulation's "backfill" cheats by reading the source list.
- A lost tail needs an externally known high LSN (e.g. source log position) to be seen at all.
- Same `(key, lsn)` with a different payload is not flagged as a conflict.
- Tombstones are kept until the contiguous low watermark passes them; with a permanent gap they are never purged.
- One writer, one table, in-memory or single-file sqlite; the LSN is a single global integer, not a per-partition vector.
- Uniform random keys and uniform delivery delay; real traffic is skewed and bursty, so real baseline divergence could be higher or lower.
- CI is configured (Python 3.11 and 3.13) but has not run.
