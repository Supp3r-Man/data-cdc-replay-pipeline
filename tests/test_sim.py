import subprocess
import sys
from pathlib import Path

from cdc_replay.sim import (contention_table, crash_resume_check, format_contention, format_gaps,
                            format_grid, gap_table, grid, run_once)

ROOT = Path(__file__).resolve().parent.parent


def test_ours_never_diverges_without_lost_events():
    for dup, window in [(0, 0), (.3, 50), (.1, 5), (.3, 0), (0, 50)]:
        for seed in range(3):
            assert run_once(seed, dup, window)["ours"] == 0, (dup, window, seed)


def test_naive_matches_ours_on_clean_stream_and_diverges_when_messy():
    clean = run_once(0, 0, 0)
    assert clean["naive"] == 0 and clean["ours"] == 0
    messy = run_once(0, .2, 25)
    assert messy["naive"] > 0 and messy["dedupe"] > 0
    r = run_once(1, 0.0, 25)          # dedupe alone does not fix reordering
    assert r["dedupe"] == r["naive"] > 0


def test_run_once_is_deterministic():
    assert run_once(2, .2, 25, drop=.03) == run_once(2, .2, 25, drop=.03)


def test_lost_events_cause_divergence_that_is_all_explained_and_detected():
    r = run_once(0, .2, 25, drop=.10)
    assert r["dropped"] > 0 and r["ours"] > 0
    assert r["unexplained"] == 0 and r["detected_all"]


def test_backfill_of_detected_gaps_restores_exact_state():
    for seed in range(3):
        assert run_once(seed, .2, 25, drop=.10)["after_backfill"] == 0


def test_lost_tail_is_invisible_without_a_high_watermark():
    hits = [run_once(s, 0, 0, drop=.10)["undetected_no_hwm"] for s in range(20)]
    assert max(hits) > 0


def test_grid_and_formatting_shape():
    assert crash_resume_check()
    rows = contention_table(seeds=range(2), keys=(10, 600))
    assert rows[0]["naive"] > rows[1]["naive"] and all(r["ours"] == 0 for r in rows)
    assert "events/key" in format_contention(rows)
    rows = grid(seeds=range(1), dups=(0.0, 0.2), windows=(0, 10))
    assert len(rows) == 4
    text = format_grid(rows)
    assert "naive+dedupe" in text and text.count("\n") == 5
    assert "unexplained" in format_gaps(gap_table(seeds=range(1), drops=(0.0, 0.1)))


def test_demo_cli_prints_tables():
    p = subprocess.run([sys.executable, "-m", "cdc_replay", "demo", "--seeds", "1"], cwd=ROOT,
                       capture_output=True, text=True, timeout=300)
    assert p.returncode == 0
    assert "naive+dedupe" in p.stdout and "= True" in p.stdout
