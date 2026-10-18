"""Idempotent, order-independent CDC apply on sqlite3, plus checkpointing, gaps and reconciliation."""
from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass

from .events import OPS

SCHEMA = """
CREATE TABLE IF NOT EXISTS target(key TEXT PRIMARY KEY, value TEXT, lsn INTEGER NOT NULL,
                                  deleted INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS applied(key TEXT NOT NULL, lsn INTEGER NOT NULL, PRIMARY KEY(key, lsn));
CREATE TABLE IF NOT EXISTS checkpoint(id INTEGER PRIMARY KEY CHECK(id = 1), position INTEGER NOT NULL);
"""


class CrashError(Exception):
    """Injected failure in the middle of a batch."""


def connect(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def apply_event(conn, ev) -> str:
    """Returns 'applied', 'stale' (older than the row's LSN) or 'duplicate' (same key+lsn seen)."""
    if ev.op not in OPS:
        raise ValueError(f"unknown op {ev.op!r}")
    cur = conn.execute("INSERT OR IGNORE INTO applied(key, lsn) VALUES (?, ?)", (ev.key, ev.lsn))
    if cur.rowcount == 0:
        return "duplicate"
    row = conn.execute("SELECT lsn FROM target WHERE key = ?", (ev.key,)).fetchone()
    if row and row[0] >= ev.lsn:
        return "stale"
    conn.execute(
        "INSERT INTO target(key, value, lsn, deleted) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value, lsn = excluded.lsn, "
        "deleted = excluded.deleted",
        (ev.key, ev.value, ev.lsn, 1 if ev.op == "delete" else 0))
    return "applied"


def get_checkpoint(conn) -> int:
    row = conn.execute("SELECT position FROM checkpoint WHERE id = 1").fetchone()
    return row[0] if row else 0


def set_checkpoint(conn, position: int) -> None:
    conn.execute("INSERT INTO checkpoint(id, position) VALUES (1, ?) "
                 "ON CONFLICT(id) DO UPDATE SET position = excluded.position", (position,))


def replay(conn, stream, batch_size: int = 100, crash_at: int | None = None) -> Counter:
    """Apply `stream` from the stored checkpoint. Each batch and its checkpoint commit together,
    so a crash mid-batch rolls the whole batch back and a resume re-reads it."""
    pos = get_checkpoint(conn)
    counts: Counter = Counter()
    while pos < len(stream):
        batch = stream[pos:pos + batch_size]
        batch_counts: Counter = Counter()
        try:
            for i, ev in enumerate(batch):
                if crash_at is not None and pos + i == crash_at:
                    raise CrashError(f"injected crash at stream offset {crash_at}")
                batch_counts[apply_event(conn, ev)] += 1
            set_checkpoint(conn, pos + len(batch))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        counts.update(batch_counts)
        pos += len(batch)
    return counts


def backfill(conn, events) -> Counter:
    """Apply events fetched out of band (e.g. re-read from the source for a detected gap)."""
    counts = Counter(apply_event(conn, ev) for ev in events)
    conn.commit()
    return counts


def find_gaps(conn, upto: int | None = None, start: int = 1) -> list[tuple[int, int]]:
    """Missing LSN ranges (inclusive) in [start, upto]. Without `upto`, a lost tail is invisible."""
    lsns = [r[0] for r in conn.execute("SELECT DISTINCT lsn FROM applied ORDER BY lsn")]
    hi = upto if upto is not None else (lsns[-1] if lsns else 0)
    gaps, expect = [], start
    for lsn in lsns:
        if lsn < start:
            continue
        if lsn > hi:
            break
        if lsn > expect:
            gaps.append((expect, lsn - 1))
        expect = lsn + 1
    if expect <= hi:
        gaps.append((expect, hi))
    return gaps


def low_watermark(conn) -> int:
    """Highest LSN such that every LSN from 1 up to it has been received."""
    gaps = find_gaps(conn)
    if gaps:
        return gaps[0][0] - 1
    row = conn.execute("SELECT MAX(lsn) FROM applied").fetchone()
    return row[0] or 0


def purge_tombstones(conn) -> int:
    """Drop tombstones at or below the low watermark: nothing older can still arrive."""
    wm = low_watermark(conn)
    cur = conn.execute("DELETE FROM target WHERE deleted = 1 AND lsn <= ?", (wm,))
    conn.commit()
    return cur.rowcount


def visible_state(conn) -> dict[str, str]:
    return dict(conn.execute("SELECT key, value FROM target WHERE deleted = 0"))


def dump(conn) -> list[tuple]:
    """Full target table including tombstones, for exact-equality comparisons."""
    return conn.execute("SELECT key, value, lsn, deleted FROM target ORDER BY key").fetchall()


@dataclass
class Report:
    missing: list[str]      # in source, not live in target
    extra: list[str]        # live in target, not in source
    mismatched: list[str]   # present in both with different values

    @property
    def divergent_keys(self) -> list[str]:
        return sorted(set(self.missing) | set(self.extra) | set(self.mismatched))

    @property
    def divergent(self) -> int:
        return len(self.divergent_keys)

    @property
    def ok(self) -> bool:
        return self.divergent == 0


def compare_states(target: dict, snapshot: dict) -> Report:
    return Report(
        missing=sorted(k for k in snapshot if k not in target),
        extra=sorted(k for k in target if k not in snapshot),
        mismatched=sorted(k for k in snapshot if k in target and target[k] != snapshot[k]))


def reconcile(conn, snapshot: dict) -> Report:
    return compare_states(visible_state(conn), snapshot)


def naive_apply(stream, dedupe: bool = False) -> dict[str, str]:
    """Fair baseline: sqlite upsert / delete in arrival order. dedupe=True adds a seen-set on
    (key, lsn), the obvious first fix, but still no LSN comparison and no tombstones."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE naive(key TEXT PRIMARY KEY, value TEXT)")
    seen = set()
    for ev in stream:
        if dedupe:
            if (ev.key, ev.lsn) in seen:
                continue
            seen.add((ev.key, ev.lsn))
        if ev.op == "delete":
            conn.execute("DELETE FROM naive WHERE key = ?", (ev.key,))
        else:
            conn.execute("INSERT OR REPLACE INTO naive(key, value) VALUES (?, ?)", (ev.key, ev.value))
    return dict(conn.execute("SELECT key, value FROM naive"))
