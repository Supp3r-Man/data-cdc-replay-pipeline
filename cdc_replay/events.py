"""Synthetic change events, the source-of-truth reference, and a seeded delivery-fault injector."""
from __future__ import annotations

import random
from typing import NamedTuple


class Event(NamedTuple):
    lsn: int          # log sequence number, unique and increasing in the source
    key: str
    op: str           # insert | update | delete
    value: str | None  # full row image; None for delete


OPS = ("insert", "update", "delete")


def generate(seed: int, n_keys: int = 150, n_events: int = 1500) -> list[Event]:
    """Valid source history: insert only when absent, update/delete only when live. LSNs are 1..n."""
    rng = random.Random(seed)
    live: set[str] = set()
    events = []
    for lsn in range(1, n_events + 1):
        key = f"acct-{rng.randrange(n_keys):04d}"
        if key not in live:
            op = "insert"
        else:
            op = "delete" if rng.random() < 0.2 else "update"
        if op == "delete":
            live.discard(key)
            value = None
        else:
            live.add(key)
            value = f"v{lsn}-{rng.randrange(10000)}"
        events.append(Event(lsn, key, op, value))
    return events


def reference_state(events) -> dict[str, str]:
    """Trivial reference: apply in LSN order to a dict. This is the source of truth."""
    state: dict[str, str] = {}
    for e in sorted(events, key=lambda e: e.lsn):
        if e.op == "delete":
            state.pop(e.key, None)
        else:
            state[e.key] = e.value
    return state


def perturb(events, seed: int, dup_rate: float = 0.0, window: int = 0,
            drop_rate: float = 0.0, burst: int = 3):
    """Return (arrival_stream, dropped_lsns).

    drop: with prob drop_rate an event starts a burst of 1..burst consecutive lost events (lost in
    every copy). duplicate: each kept event is redelivered with prob dup_rate. reorder: every
    delivery gets arrival time index + U(0, window), so inversions span fewer than `window` LSNs.
    """
    rng = random.Random(seed)
    kept, dropped = [], set()
    i = 0
    while i < len(events):
        if drop_rate and rng.random() < drop_rate:
            n = rng.randint(1, burst)
            dropped.update(e.lsn for e in events[i:i + n])
            i += n
            continue
        kept.append(events[i])
        i += 1
    items = []
    for idx, e in enumerate(kept):
        items.append((idx + rng.uniform(0, window), 0, idx, e))
        if dup_rate and rng.random() < dup_rate:
            items.append((idx + rng.uniform(0, window), 1, idx, e))
    items.sort(key=lambda t: t[:3])
    return [t[3] for t in items], dropped
