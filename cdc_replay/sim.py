"""Seeded simulation: naive vs idempotent LWW apply, divergence from the source snapshot."""
from __future__ import annotations

from statistics import mean

from .applier import (CrashError, backfill, compare_states, connect, dump, find_gaps, naive_apply,
                      reconcile, replay)
from .events import generate, perturb, reference_state

DUPS = (0.0, 0.1, 0.2, 0.3)
WINDOWS = (0, 5, 10, 25, 50)
DROPS = (0.0, 0.01, 0.03, 0.10)


def run_once(seed, dup, window, drop=0.0, n_keys=150, n_events=1500, batch=100) -> dict:
    events = generate(seed, n_keys, n_events)
    truth = reference_state(events)
    stream, dropped = perturb(events, seed + 1000, dup, window, drop)
    total = len({e.key for e in events})
    conn = connect()
    replay(conn, stream, batch)
    rep = reconcile(conn, truth)
    gaps = find_gaps(conn, upto=n_events)
    detected = {n for a, b in gaps for n in range(a, b + 1)}
    tail_blind = {n for a, b in find_gaps(conn) for n in range(a, b + 1)}
    key_of = {e.lsn: e.key for e in events}
    dropped_keys = {key_of[n] for n in dropped}
    out = {
        "naive": compare_states_div(naive_apply(stream), truth) / total,
        "dedupe": compare_states_div(naive_apply(stream, dedupe=True), truth) / total,
        "ours": rep.divergent / total,
        "dropped": len(dropped),
        "ranges": len(gaps),
        "detected_all": detected == dropped,
        "undetected_no_hwm": len(dropped - tail_blind),
        "unexplained": len(set(rep.divergent_keys) - dropped_keys),
    }
    backfill(conn, [e for e in events if e.lsn in detected])
    out["after_backfill"] = reconcile(conn, truth).divergent / total
    return out


def compare_states_div(state, truth) -> int:
    return compare_states(state, truth).divergent


def grid(seeds=range(5), dups=DUPS, windows=WINDOWS) -> list[dict]:
    rows = []
    for d in dups:
        for w in windows:
            runs = [run_once(s, d, w) for s in seeds]
            rows.append({"dup": d, "window": w, **{k: mean(r[k] for r in runs) for k in ("naive", "dedupe", "ours")}})
    return rows


def gap_table(seeds=range(5), drops=DROPS, dup=0.2, window=25) -> list[dict]:
    rows = []
    for dr in drops:
        runs = [run_once(s, dup, window, drop=dr) for s in seeds]
        rows.append({"drop": dr, "n": len(runs),
                     **{k: mean(r[k] for r in runs) for k in
                        ("dropped", "ranges", "ours", "naive", "unexplained", "after_backfill",
                         "undetected_no_hwm")},
                     "detected_all": sum(r["detected_all"] for r in runs)})
    return rows


def contention_table(seeds=range(5), keys=(10, 30, 150, 600), dup=0.2, window=25) -> list[dict]:
    """Naive divergence depends on how often one key is rewritten; same 1500 events, fewer keys."""
    rows = []
    for k in keys:
        runs = [run_once(s, dup, window, n_keys=k) for s in seeds]
        rows.append({"keys": k, **{m: mean(r[m] for r in runs) for m in ("naive", "dedupe", "ours")}})
    return rows


def format_contention(rows) -> str:
    lines = ["keys   events/key   naive   naive+dedupe   ours", "-" * 46]
    for r in rows:
        lines.append(f"{r['keys']:<6} {1500 / r['keys']:<12.0f} {pct(r['naive'])}   {pct(r['dedupe'])}       {pct(r['ours'])}")
    return "\n".join(lines)


def crash_resume_check(seed=0, crash_at=777, dup=0.2, window=25) -> bool:
    events = generate(seed)
    stream, _ = perturb(events, seed + 1000, dup, window)
    a = connect()
    replay(a, stream)
    b = connect()
    try:
        replay(b, stream, crash_at=crash_at)
    except CrashError:
        pass
    replay(b, stream)
    return dump(a) == dump(b)


def pct(x) -> str:
    return f"{100 * x:5.1f}%"


def format_grid(rows) -> str:
    lines = ["dup    window   naive   naive+dedupe   ours", "-" * 44]
    for r in rows:
        lines.append(f"{r['dup']:<6.0%} {r['window']:<8} {pct(r['naive'])}   {pct(r['dedupe'])}       {pct(r['ours'])}")
    return "\n".join(lines)


def format_gaps(rows) -> str:
    lines = ["drop   lost LSNs  ranges  ours     +backfill  naive    detected(all runs)  tail-blind  unexplained",
             "-" * 100]
    for r in rows:
        lines.append(f"{r['drop']:<6.0%} {r['dropped']:<11.1f} {r['ranges']:<7.1f} {pct(r['ours'])}   "
                     f"{pct(r['after_backfill'])}    {pct(r['naive'])}   {r['detected_all']}/{r['n']:<16} "
                     f"{r['undetected_no_hwm']:<11.1f} {r['unexplained']:.1f}")
    return "\n".join(lines)
