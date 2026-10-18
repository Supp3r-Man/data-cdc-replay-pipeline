from __future__ import annotations

import argparse
import sys

from .sim import (contention_table, crash_resume_check, format_contention, format_gaps, format_grid,
                  gap_table, grid)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="cdc_replay")
    p.add_argument("command", choices=["demo"])
    p.add_argument("--seeds", type=int, default=10)
    args = p.parse_args(argv)
    seeds = range(args.seeds)
    print(f"Synthetic CDC replay: 1500 events over 150 keys, mean over {args.seeds} seeds.")
    print("Divergence = share of keys whose final state differs from the source snapshot.\n")
    print("1) No lost events: duplicates x reorder window")
    print(format_grid(grid(seeds)))
    print("\n2) Lost LSN bursts (dup 20%, window 25): where the replay still fails")
    print(format_gaps(gap_table(seeds)))
    print("\n3) Same stream (dup 20%, window 25), fewer keys = more rewrites per key")
    print(format_contention(contention_table(seeds)))
    print(f"\n4) Crash at stream offset 777, resume: identical table to clean run = {crash_resume_check()}")
    return 0


if __name__ == "__main__":
    for s in (sys.stdout, sys.stderr):
        s.reconfigure(encoding="utf-8", newline="\n")
    sys.exit(main())
