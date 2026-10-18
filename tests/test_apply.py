import pytest

from cdc_replay.applier import (CrashError, apply_event, backfill, connect, dump, find_gaps,
                                get_checkpoint, low_watermark, purge_tombstones, reconcile, replay,
                                visible_state)
from cdc_replay.events import Event, generate, perturb

I = lambda lsn, k="a", v=None: Event(lsn, k, "insert", v or f"v{lsn}")
U = lambda lsn, k="a", v=None: Event(lsn, k, "update", v or f"v{lsn}")
D = lambda lsn, k="a": Event(lsn, k, "delete", None)


def test_insert_update_in_order():
    c = connect()
    assert apply_event(c, I(1)) == "applied"
    assert apply_event(c, U(2)) == "applied"
    assert visible_state(c) == {"a": "v2"}


def test_delete_leaves_tombstone_not_visible():
    c = connect()
    apply_event(c, I(1))
    apply_event(c, D(2))
    assert visible_state(c) == {}
    assert dump(c) == [("a", None, 2, 1)]


def test_duplicate_is_skipped():
    c = connect()
    apply_event(c, I(1))
    assert apply_event(c, I(1)) == "duplicate"
    assert dump(c) == [("a", "v1", 1, 0)]


def test_late_duplicate_does_not_overwrite_newer_state():
    c = connect()
    for e in (I(1), U(2), U(3)):
        apply_event(c, e)
    assert apply_event(c, U(2)) == "duplicate"
    assert visible_state(c) == {"a": "v3"}


def test_last_writer_wins_by_lsn_not_arrival():
    c = connect()
    apply_event(c, U(3))
    assert apply_event(c, U(2)) == "stale"
    apply_event(c, I(1))
    assert visible_state(c) == {"a": "v3"}
    assert find_gaps(c) == []        # stale events still count as received


def test_delete_before_insert_tombstone_blocks_late_insert():
    c = connect()
    assert apply_event(c, D(5)) == "applied"
    assert apply_event(c, I(2)) == "stale"
    assert apply_event(c, U(4)) == "stale"
    assert visible_state(c) == {}


def test_reinsert_after_delete_with_higher_lsn_is_live():
    c = connect()
    apply_event(c, D(5))
    apply_event(c, I(9, v="again"))
    assert visible_state(c) == {"a": "again"}


def test_unknown_op_rejected_without_touching_ledger():
    c = connect()
    with pytest.raises(ValueError):
        apply_event(c, Event(1, "a", "truncate", None))
    assert find_gaps(c, upto=1) == [(1, 1)]


def test_ledger_is_keyed_by_key_and_lsn():
    c = connect()
    assert apply_event(c, I(1, "a")) == "applied"
    assert apply_event(c, I(1, "b")) == "applied"
    assert apply_event(c, I(1, "a")) == "duplicate"


def _stream(seed=0, **kw):
    return perturb(generate(seed), seed + 1000, **kw)[0]


def test_replay_stores_checkpoint_and_second_call_is_noop():
    c, s = connect(), _stream(dup_rate=0.2, window=10)
    counts = replay(c, s, batch_size=64)
    assert get_checkpoint(c) == len(s)
    assert sum(counts.values()) == len(s)
    assert replay(c, s, batch_size=64) == {}


def test_crash_mid_batch_rolls_back_whole_batch():
    c, s = connect(), _stream()
    with pytest.raises(CrashError):
        replay(c, s, batch_size=100, crash_at=250)
    assert get_checkpoint(c) == 200
    assert c.execute("SELECT COUNT(*) FROM applied").fetchone()[0] == 200   # failed batch invisible


def test_resume_processes_only_remaining_events():
    c, s = connect(), _stream()
    with pytest.raises(CrashError):
        replay(c, s, batch_size=100, crash_at=250)
    counts = replay(c, s, batch_size=100)
    assert sum(counts.values()) == len(s) - 200


def test_crash_then_resume_equals_clean_run(tmp_path):
    s = _stream(dup_rate=0.2, window=25)
    clean = connect()
    replay(clean, s)
    for crash_at in (0, 1, 99, 100, 101, 750, 1499):
        c = connect(str(tmp_path / f"t{crash_at}.db"))
        with pytest.raises(CrashError):
            replay(c, s, crash_at=crash_at)
        c.close()
        c2 = connect(str(tmp_path / f"t{crash_at}.db"))   # new connection, as after a restart
        replay(c2, s)
        assert dump(c2) == dump(clean), crash_at


def test_replaying_whole_stream_again_changes_nothing():
    c, s = connect(), _stream(dup_rate=0.3, window=50)
    replay(c, s)
    before = dump(c)
    c.execute("UPDATE checkpoint SET position = 0")
    c.commit()
    counts = replay(c, s)
    assert dump(c) == before
    assert counts["applied"] == 0 and counts["stale"] == 0


def test_batch_size_does_not_change_result():
    s = _stream(dup_rate=0.2, window=25)
    a = connect()
    replay(a, s, batch_size=100)
    for bs in (1, 7, 1000):
        b = connect()
        replay(b, s, batch_size=bs)
        assert dump(a) == dump(b), bs


def _ledger(lsns):
    c = connect()
    for n in lsns:
        apply_event(c, U(n, k=f"k{n}"))
    return c


def test_find_gaps_ranges():
    assert find_gaps(_ledger([1, 2, 5, 6, 9])) == [(3, 4), (7, 8)]
    assert find_gaps(_ledger([3, 1, 2])) == []
    c = _ledger([3, 4])
    assert find_gaps(c) == [(1, 2)]
    assert find_gaps(c, upto=6) == [(1, 2), (5, 6)]
    assert find_gaps(connect(), upto=4) == [(1, 4)]
    assert find_gaps(connect()) == []


def test_low_watermark():
    assert low_watermark(_ledger([1, 2, 3, 7])) == 3
    assert low_watermark(_ledger([1, 2, 3])) == 3
    assert low_watermark(connect()) == 0


def test_purge_removes_only_tombstones_below_watermark():
    c = connect()
    for e in (I(1, "a"), D(2, "a"), I(3, "b"), D(4, "b"), I(6, "c"), D(7, "c")):
        apply_event(c, e)            # LSN 5 never arrives: watermark is 4
    assert purge_tombstones(c) == 2
    assert [r[0] for r in dump(c)] == ["c"]


def test_purged_key_is_not_resurrected_by_late_duplicate():
    c = connect()
    for e in (I(1), D(2)):
        apply_event(c, e)
    purge_tombstones(c)
    assert apply_event(c, I(1)) == "duplicate"
    assert visible_state(c) == {}


def test_reconcile_ok_and_tombstones_are_not_extra():
    c = connect()
    apply_event(c, I(1, "a"))
    apply_event(c, I(2, "b"))
    apply_event(c, D(3, "b"))
    assert reconcile(c, {"a": "v1"}).ok


def test_reconcile_reports_missing_extra_mismatched():
    c = connect()
    apply_event(c, I(1, "a", "x"))
    apply_event(c, I(2, "b", "y"))
    r = reconcile(c, {"a": "WRONG", "c": "z"})
    assert r.mismatched == ["a"] and r.extra == ["b"] and r.missing == ["c"]
    assert r.divergent == 3 and not r.ok


def test_backfill_repairs_a_gap():
    c = connect()
    apply_event(c, I(1, "a"))
    apply_event(c, I(3, "b"))
    assert find_gaps(c) == [(2, 2)]
    assert backfill(c, [I(2, "c")])["applied"] == 1
    assert find_gaps(c) == [] and set(visible_state(c)) == {"a", "b", "c"}
