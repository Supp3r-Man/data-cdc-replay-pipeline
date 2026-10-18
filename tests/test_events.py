import re

from cdc_replay.events import Event, generate, perturb, reference_state


def test_generate_is_deterministic():
    assert generate(3) == generate(3)
    assert generate(3) != generate(4)


def test_lsns_are_contiguous_from_one():
    ev = generate(1, n_events=300)
    assert [e.lsn for e in ev] == list(range(1, 301))


def test_history_is_valid():
    live = set()
    for e in generate(2):
        if e.op == "insert":
            assert e.key not in live
            live.add(e.key)
        else:
            assert e.key in live
            if e.op == "delete":
                live.remove(e.key)
                assert e.value is None


def test_reference_state_hand_example():
    ev = [Event(2, "a", "update", "x2"), Event(1, "a", "insert", "x1"),
          Event(3, "b", "insert", "y"), Event(4, "b", "delete", None)]
    assert reference_state(ev) == {"a": "x2"}


def test_perturb_noop_keeps_order():
    ev = generate(1, n_events=200)
    stream, dropped = perturb(ev, 5)
    assert stream == ev and not dropped


def test_full_duplication_doubles_stream_and_loses_nothing():
    ev = generate(1, n_events=200)
    stream, _ = perturb(ev, 5, dup_rate=1.0)
    assert len(stream) == 400
    assert {e.lsn for e in stream} == {e.lsn for e in ev}


def test_reorder_displacement_is_bounded_by_window():
    ev = generate(1, n_events=400)
    w = 10
    stream, _ = perturb(ev, 5, window=w)
    assert stream != ev
    pos = {e.lsn: i for i, e in enumerate(stream)}
    for a in range(1, 401):
        for b in range(a + 1, min(a + 40, 401)):
            if pos[b] < pos[a]:
                assert b - a < w


def test_drops_are_absent_from_every_copy():
    ev = generate(1, n_events=500)
    stream, dropped = perturb(ev, 5, dup_rate=0.5, window=5, drop_rate=0.05)
    assert dropped
    assert not dropped & {e.lsn for e in stream}
    assert {e.lsn for e in stream} | dropped == {e.lsn for e in ev}


def test_synthetic_data_has_no_card_ssn_or_email_shapes():
    text = " ".join(f"{e.key} {e.value}" for e in generate(1))
    assert not re.search(r"\b\d{13,19}\b", text)
    assert not re.search(r"\b\d{3}-\d{2}-\d{4}\b", text)
    assert "@" not in text
