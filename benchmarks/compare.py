"""Compare two benchmark CSVs (baseline vs redesign).

Usage:
    python compare.py results/baseline.csv results/redesign.csv
"""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def load(path: Path) -> dict[tuple[str, str], list[float]]:
    out: dict[tuple[str, str], list[float]] = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            key = (row["operation"], row["scale"])
            out[key].append(float(row["wall_s"]))
    return out


def _fmt(seconds: float) -> str:
    if seconds < 0.001:
        return f"{seconds * 1_000_000:7.0f} µs"
    if seconds < 1.0:
        return f"{seconds * 1000:7.1f} ms"
    if seconds < 60:
        return f"{seconds:7.2f} s "
    return f"{seconds / 60:7.2f} m "


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python compare.py baseline.csv redesign.csv", file=sys.stderr)
        sys.exit(2)

    before = load(Path(sys.argv[1]))
    after = load(Path(sys.argv[2]))
    keys = sorted(set(before) | set(after))

    print(f"{'operation':<28} {'scale':>6} {'before':>12} {'after':>12} {'speedup':>10}")
    print("-" * 72)
    for op, scale in keys:
        b = before.get((op, scale))
        a = after.get((op, scale))
        b_med = statistics.median(b) if b else float("nan")
        a_med = statistics.median(a) if a else float("nan")
        if b and a and a_med > 0:
            ratio = f"{b_med / a_med:>8.2f}x"
        else:
            ratio = "       -"
        print(f"{op:<28} {scale:>6} {_fmt(b_med)} {_fmt(a_med)} {ratio:>10}")


if __name__ == "__main__":
    main()
