# STEPS

**Part A** is what I did, in order. **Part B** is how you run it.

---

## Part A: steps taken

1. **Fixed the semantics first.** Events are full row images with a global LSN. Correct end state = apply in LSN order. That one-line reference (`reference_state`) became both the source of truth and the oracle for the property tests.
2. **Wrote the generator** (`events.py`): a valid history (insert only when absent, update/delete only when live), contiguous LSNs, fictional `acct-NNNN` keys, a test that scans for card, SSN and email shapes.
3. **Wrote the fault injector**: bursts of 1-3 lost LSNs, duplicates, and reorder by giving each delivery arrival time `index + U(0, window)`, which bounds how far an event can move (a test checks the bound).
4. **Wrote the applier on sqlite3**: `target` table with a `deleted` tombstone flag and the event's LSN, an `applied(key, lsn)` ledger, a `checkpoint` row. The ledger is written first with `INSERT OR IGNORE`; a stale event (older than the row) is skipped but stays in the ledger.
5. **Checkpoint and crash**: each batch and its checkpoint commit in one transaction; `crash_at` raises in the middle of a batch. Tests crash at 7 offsets using a file database and a brand-new connection, and compare the full table (tombstones included) with a clean run.
6. **Gaps, watermark, purge, reconcile, backfill.** Purge only drops tombstones at or below the contiguous low watermark.
7. **Wrote the baselines** deliberately fair: an upsert/delete in arrival order (a test checks it is correct on a clean stream), and a second one with a `(key, lsn)` seen-set.
8. **Wrote the simulation and demo**, then the tests: 44, including 5 seeded property-style tests against the trivial reference.
9. **Mutation-checked 8 behaviours** on fresh copies (script reports PATTERN MISSED if a replace does not apply). All 8 killed on the first run.
10. **Wrote README and this file**, committed locally. Not pushed; CI has not run.

Problems and choices along the way:
- First run of the demo: the baseline looked weak (1-5% divergence). I did not tune it to look worse. Divergence turns out to depend on rewrites per key, so I added a contention table (39% at 150 events per key).
- My first draft of the burst-drop code in `perturb` had leftover nonsense lines; replaced with a plain `n = randint(1, burst)` slice before the first run.
- One test asserted a wrong gap for a stale event (a stale event still counts as received); fixed the test, not the code. A threshold of 5% on one seed was also too tight, replaced with `> 0`.
- The backfill in the simulation reads the source list, so it shows detection is correct, not that recovery is automatic.

---

## Part B: steps for you

1. Needs Python 3.11 or newer. Nothing to install except pytest for the tests.
2. From the repo folder: `python -m pytest` (44 tests, about 4 s).
3. `python -m cdc_replay demo` prints four tables: the duplicate x window grid, the lost-LSN table, the contention table, and the crash/resume check. `--seeds 3` is faster and noisier.
4. Try it by hand:
   ```python
   from cdc_replay.applier import connect, apply_event, visible_state
   from cdc_replay.events import Event
   c = connect()
   apply_event(c, Event(5, "a", "delete", None))   # delete arrives first: tombstone
   apply_event(c, Event(2, "a", "insert", "x"))    # late insert is 'stale'
   print(visible_state(c))                         # {}
   ```
5. To change the traffic shape edit the defaults in `cdc_replay/sim.py` (`run_once`: keys, events, batch) or the grids (`DUPS`, `WINDOWS`, `DROPS`).
6. To push later: `git remote add origin ...; git push -u origin main`, then check the Actions tab; only then can the CI claim be made.
