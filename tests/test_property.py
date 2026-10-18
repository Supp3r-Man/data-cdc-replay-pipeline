"""Seeded property-style tests against the trivial reference (sort by LSN, apply to a dict)."""
import random

from cdc_replay.applier import connect, find_gaps, naive_apply, reconcile, replay
from cdc_replay.events import generate, perturb, reference_state


def _case(seed):
    rng = random.Random(seed)
    events = generate(seed, n_keys=rng.choice([5, 40, 150]), n_events=rng.choice([50, 400, 900]))
    dup, window = rng.choice([0, .1, .3]), rng.choice([0, 3, 25, 50])
    return events, perturb(events, seed * 7 + 1, dup, window)[0], rng.choice([1, 13, 100])


def test_matches_reference_under_dups_and_reordering():
    for seed in range(30):
        events, stream, batch = _case(seed)
        c = connect()
        replay(c, stream, batch)
        assert reconcile(c, reference_state(events)).ok, seed


def test_total_shuffle_with_duplicates_still_matches_reference():
    for seed in range(10):
        events = generate(seed, n_keys=20, n_events=300)
        stream = events + random.Random(seed).sample(events, 100)
        random.Random(seed + 1).shuffle(stream)
        c = connect()
        replay(c, stream)
        assert reconcile(c, reference_state(events)).ok, seed


def test_with_drops_result_equals_reference_on_received_events():
    for seed in range(15):
        events = generate(seed, n_events=600)
        stream, dropped = perturb(events, seed + 99, dup_rate=.2, window=25, drop_rate=.05)
        c = connect()
        replay(c, stream)
        received = [e for e in events if e.lsn not in dropped]
        assert reconcile(c, reference_state(received)).ok, seed
        detected = {n for a, b in find_gaps(c, upto=600) for n in range(a, b + 1)}
        assert detected == dropped, seed


def test_naive_is_correct_on_clean_in_order_stream():
    for seed in range(15):
        events = generate(seed, n_events=500)
        assert naive_apply(events) == reference_state(events)
        assert naive_apply(events, dedupe=True) == reference_state(events)


def test_naive_actually_breaks_under_reordering():
    wrong = 0
    for seed in range(10):
        events = generate(seed, n_events=800)
        stream = perturb(events, seed, dup_rate=.2, window=25)[0]
        wrong += naive_apply(stream, dedupe=True) != reference_state(events)
    assert wrong >= 9
