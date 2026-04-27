"""Salesy / brand-compliant plots for the energydb benchmark.

Produces six PNGs under ``plots/``:

    throughput_light.png         throughput_dark.png
    timing_breakdown_light.png   timing_breakdown_dark.png
    before_vs_after_light.png    before_vs_after_dark.png

End-to-end — exercises energydb's PG resolution + unit conversion + hierarchy
join on top of the timedb phases, so the timing breakdown separates them.

The plots use the rebase-energy brand ``style`` package at
``/home/kunruh/src/rebase-energy/style`` if available; otherwise they fall
back to a plain matplotlib theme.

Usage:
    python backend/libs/energydb/benchmarks/plot_results.py
    python backend/libs/energydb/benchmarks/plot_results.py results/my_run.csv
"""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Attempt to import the branded style package ──────────────────────────────

_STYLE_LOCATION = Path("/home/kunruh/src/rebase-energy")
_USE_BRAND_STYLE = (_STYLE_LOCATION / "style").is_dir()
if _USE_BRAND_STYLE and str(_STYLE_LOCATION) not in sys.path:
    sys.path.insert(0, str(_STYLE_LOCATION))

try:
    from style import (  # noqa: E402
        DARK_GREEN,
        GREEN_BRIGHT,
        HERO_COLOR,
        MUTED_COLOR,
        RED,
        set_style,
    )
    from style.chrome import apply_background, footer, headline, overline, subtitle  # noqa: E402
    from style.formatters import fmt_rows, fmt_throughput, fmt_time  # noqa: E402
    from style.palettes import dark_bg_for, line_accents, phase_palette  # noqa: E402
except ImportError:
    _USE_BRAND_STYLE = False


# ── Fallbacks if the brand style isn't available ─────────────────────────────


if not _USE_BRAND_STYLE:
    HERO_COLOR = "#0D9373"
    GREEN_BRIGHT = "#03C497"
    DARK_GREEN = "#06503D"
    MUTED_COLOR = "#247D67"
    RED = "#CC3B35"

    def fmt_throughput(r):
        if r >= 1e6:
            return f"{r / 1e6:.1f} M/s"
        if r >= 1e3:
            return f"{r / 1e3:.0f} K/s"
        return f"{r:.0f} /s"

    def fmt_rows(r):
        if r >= 1e6:
            return f"{r / 1e6:.1f}M"
        if r >= 1e3:
            return f"{r / 1e3:.0f}K"
        return f"{r:.0f}"

    def fmt_time(s):
        if s < 0.001:
            return f"{s * 1e6:.0f} µs"
        if s < 1:
            return f"{s * 1000:.0f} ms"
        return f"{s:.1f}s"

    def set_style(theme):
        class _Ctx:
            grid = "#D0D0D0" if theme == "light" else "#3A3A3A"

        plt.rcParams["axes.facecolor"] = "none"
        plt.rcParams["figure.facecolor"] = "#FAFAFA" if theme == "light" else "#121619"
        plt.rcParams["text.color"] = "#0F172A" if theme == "light" else "#E6EEF2"
        plt.rcParams["axes.labelcolor"] = plt.rcParams["text.color"]
        plt.rcParams["xtick.color"] = plt.rcParams["text.color"]
        plt.rcParams["ytick.color"] = plt.rcParams["text.color"]
        plt.rcParams["axes.edgecolor"] = _Ctx.grid
        return _Ctx

    def apply_background(fig, theme):
        fig.patch.set_facecolor(plt.rcParams["figure.facecolor"])

    def overline(fig, y, text):
        fig.text(0.06, y, text.upper(), fontsize=9, color=HERO_COLOR, fontweight="bold")

    def headline(fig, y, a, b, c):
        fig.text(0.06, y, f"{a} {b} {c}", fontsize=22, fontweight="bold")

    def subtitle(fig, y, text):
        fig.text(0.06, y, text, fontsize=10, color="#64748B")

    def footer(fig):
        fig.text(0.98, 0.02, "energydb · unified events", fontsize=8,
                 color="#9CA3AF", ha="right")

    def dark_bg_for(theme):
        return theme == "dark"

    def line_accents(dark_bg):
        return (GREEN_BRIGHT, HERO_COLOR) if dark_bg else (HERO_COLOR, DARK_GREEN)

    def phase_palette(phases, dark_bg):
        base = ["#18A058", "#3DDC84", "#0B7FAF", "#F59E0B", "#94A3B8", "#EC4899", "#A855F7"]
        return {p: base[i % len(base)] for i, p in enumerate(phases)}


# ── Configuration ────────────────────────────────────────────────────────────

RESULTS_DIR = Path(__file__).parent / "results"
DEFAULT_CSV = RESULTS_DIR / "redesign.csv"
BASELINE_CSV = RESULTS_DIR / "baseline.csv"
PLOTS_DIR = Path(__file__).parent / "plots"

# Phase lists kept at ≤4 entries so the brand's green-ramp palette fits the
# 5-bar stack (4 phases + overhead). CH-side insert phases are combined into
# one "CH insert" bar; Arrow→Polars steps on read likewise.
WRITE_PHASES = [
    ("Series resolve", "edb.resolve"),
    ("Runs upsert", "edb.runs_upsert"),
    ("Normalize", "write.normalize"),
    ("CH insert", ["write.events_insert", "write.run_series_insert"]),
]

READ_PHASES = [
    ("Series resolve", "edb.resolve"),
    ("SQL exec", "read.sql_exec"),
    ("Arrow → Polars", ["read.build_arrow", "read.to_polars"]),
    ("Hierarchy join", "edb.hierarchy_join"),
]

WRITE_PHASE_DESCRIPTIONS = {
    "Series resolve": "PostgreSQL lookup of series metadata (series_id, unit, retention, timeseries_type).",
    "Runs upsert": "PostgreSQL upsert into energydb.runs — run metadata off the CH critical path.",
    "Normalize": "Polars prep: cast, fill_null, lit stamps for knowledge_time/run_id/retention.",
    "CH insert": "Bulk Arrow insert into events + run_series in ClickHouse.",
    "Overhead": "Orchestration and anything outside the measured phases.",
}

READ_PHASE_DESCRIPTIONS = {
    "Series resolve": "PG subtree resolution + series metadata lookup for the scope.",
    "SQL exec": "ClickHouse query execution and Arrow transfer in one call.",
    "Arrow → Polars": "Column selection, NaN-mask, and pl.from_arrow() conversion.",
    "Hierarchy join": "Join CH values with PG hierarchy to form the returned DataFrame.",
    "Overhead": "Orchestration and anything outside the measured phases.",
}


# ── CSV loading / aggregation ────────────────────────────────────────────────


def load_rows(path: Path) -> list[dict]:
    with open(path) as f:
        return [
            {**r, "scale": int(r["scale"]), "trial": int(r["trial"]),
             "n_series": int(r["n_series"]), "n_rows": int(r["n_rows"]),
             "wall_s": float(r["wall_s"]),
             **{k: float(v) for k, v in r.items() if "." in k}}
            for r in csv.DictReader(f)
        ]


def by_op_scale(rows: list[dict]) -> dict[tuple[str, int], list[dict]]:
    out: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in rows:
        out[(r["operation"], r["scale"])].append(r)
    return out


def agg(trials: list[dict]) -> dict:
    wall = [t["wall_s"] for t in trials]
    n_rows = [t["n_rows"] for t in trials]
    a = {
        "n_series": trials[0]["n_series"],
        "n_rows_median": statistics.median(n_rows),
        "wall_median": statistics.median(wall),
        "wall_p25": np.percentile(wall, 25) if len(wall) > 1 else wall[0],
        "wall_p75": np.percentile(wall, 75) if len(wall) > 1 else wall[0],
    }
    for col in trials[0]:
        if "." in col:
            a[col] = statistics.median([t[col] for t in trials])
    return a


# ── Chart panels ─────────────────────────────────────────────────────────────


def plot_write_throughput(ax, buckets, op="forecast_write"):
    scales = sorted(s for (o, s) in buckets if o == op and s > 0)
    if not scales:
        ax.set_title("no forecast_write data")
        return
    x = np.array(scales, dtype=float)
    rows = np.array([agg(buckets[(op, s)])["n_rows_median"] for s in scales])
    med = np.array([agg(buckets[(op, s)])["wall_median"] for s in scales])
    p25 = np.array([agg(buckets[(op, s)])["wall_p25"] for s in scales])
    p75 = np.array([agg(buckets[(op, s)])["wall_p75"] for s in scales])
    tp = rows / med
    lo = rows / p75
    hi = rows / p25

    ax.fill_between(x, lo, hi, alpha=0.18, color=HERO_COLOR, linewidth=0, label="p25–p75")
    ax.plot(x, tp, "o-", color=HERO_COLOR, linewidth=2.8, markersize=8, label="Median")

    for i, (xi, yi) in enumerate(zip(x, tp, strict=False)):
        above = i % 2 == 0
        ax.annotate(
            fmt_throughput(yi), (xi, yi),
            textcoords="offset points", xytext=(0, 8 if above else -10),
            ha="center", va="bottom" if above else "top",
            fontsize=9, color=HERO_COLOR, fontweight="600",
        )

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(x)
    labels = [f"{int(s)}\n{fmt_rows(r)}" for s, r in zip(scales, rows, strict=False)]
    ax.set_xticklabels(labels, fontsize=9)
    ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    ax.set_xlabel("Customers  ·  rows written")
    ax.set_ylabel("Rows / sec")
    ax.set_title("Write throughput", pad=10)
    ax.legend(loc="lower right")


def plot_read_throughput(ax, buckets, c_latest, c_overlap):
    def _line(op, color, marker, label, above):
        scales = sorted(s for (o, s) in buckets if o == op)
        if not scales:
            return
        x = np.array(scales, dtype=float)
        rows = np.array([agg(buckets[(op, s)])["n_rows_median"] for s in scales])
        med = np.array([agg(buckets[(op, s)])["wall_median"] for s in scales])
        p25 = np.array([agg(buckets[(op, s)])["wall_p25"] for s in scales])
        p75 = np.array([agg(buckets[(op, s)])["wall_p75"] for s in scales])

        mask = rows > 0
        if mask.sum() == 0:
            return
        x, rows, med, p25, p75 = x[mask], rows[mask], med[mask], p25[mask], p75[mask]

        tp = rows / med
        lo = rows / p75
        hi = rows / p25
        ax.fill_between(x, lo, hi, alpha=0.15, color=color, linewidth=0)
        ax.plot(x, tp, marker + "-", color=color, linewidth=2.6, markersize=8, label=label)

        for idx in (0, len(x) - 1) if len(x) > 1 else (0,):
            txt = f"{fmt_throughput(tp[idx])}\n{fmt_rows(rows[idx])} rows"
            ax.annotate(
                txt, (x[idx], tp[idx]),
                textcoords="offset points", xytext=(0, 10 if above else -12),
                ha="center", va="bottom" if above else "top",
                fontsize=9, color=color, fontweight="600",
            )

    _line("read_latest", c_latest, "o", "Latest", above=True)
    _line("read_overlapping_history", c_overlap, "s", "Overlapping history", above=False)

    ax.set_xscale("log")
    ax.set_yscale("log")
    scales = sorted({s for (o, s) in buckets if o in ("read_latest", "read_overlapping_history")})
    if scales:
        ax.set_xticks(scales)
        ax.set_xticklabels([str(s) for s in scales], fontsize=9)
        ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    ax.set_xlabel("Customers")
    ax.set_ylabel("Rows / sec")
    ax.set_title("Read throughput", pad=10)
    ax.legend(loc="lower right", fontsize=9)


def _plot_phase_stack(ax, buckets, op, phases, colours, title):
    scales = sorted(s for (o, s) in buckets if o == op and s > 0)
    if not scales:
        ax.set_title(f"{title} — no data", pad=10)
        return
    xs = np.arange(len(scales))
    rows = np.array([agg(buckets[(op, s)])["n_rows_median"] for s in scales])
    wall_ms = np.array([agg(buckets[(op, s)])["wall_median"] for s in scales]) * 1000

    phase_ms = []
    for _label, col_or_cols in phases:
        cols = col_or_cols if isinstance(col_or_cols, list) else [col_or_cols]
        phase_ms.append(
            np.array([sum(agg(buckets[(op, s)]).get(c, 0.0) for c in cols) for s in scales]) * 1000
        )
    phase_sum = np.sum(phase_ms, axis=0)
    overhead_ms = np.maximum(wall_ms - phase_sum, 0)

    bottom = np.zeros_like(xs, dtype=float)
    for (label, _col), vals in zip(phases, phase_ms, strict=False):
        ax.bar(xs, vals, bottom=bottom, label=label, color=colours[label], width=0.65)
        bottom += vals
    ax.bar(xs, overhead_ms, bottom=bottom, label="Overhead",
           color=colours["Overhead"], width=0.65)

    for i, t in enumerate(wall_ms):
        ax.text(i, t + wall_ms.max() * 0.02, fmt_time(t / 1000),
                ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.set_xticks(xs)
    labels = [f"{s}  ·  {fmt_rows(r)}" for s, r in zip(scales, rows, strict=False)]
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=9)
    ax.set_xlabel("Customers  ·  rows")
    ax.set_ylabel("Median time (ms)")
    ax.set_title(title, pad=10)
    ax.legend(loc="upper left", ncol=2, fontsize=8.5)


def plot_before_vs_after(ax, before_buckets, after_buckets):
    ops = sorted({op for (op, _s) in after_buckets})
    scales_per_op = {op: sorted(s for (o, s) in after_buckets if o == op) for op in ops}

    labels: list[str] = []
    before_vals: list[float] = []
    after_vals: list[float] = []
    for op in ops:
        for s in scales_per_op[op]:
            a = agg(after_buckets[(op, s)])["wall_median"]
            b = None
            if before_buckets and (op, s) in before_buckets:
                b = agg(before_buckets[(op, s)])["wall_median"]
            labels.append(f"{op}@{s}" if s > 0 else op)
            before_vals.append(b if b is not None else float("nan"))
            after_vals.append(a)

    if not labels:
        ax.text(0.5, 0.5, "no data", ha="center", va="center",
                transform=ax.transAxes, fontsize=11)
        ax.set_xticks([])
        return

    xs = np.arange(len(labels))
    width = 0.4
    ax.bar(xs - width / 2, before_vals, width, label="baseline", color=MUTED_COLOR)
    ax.bar(xs + width / 2, after_vals, width, label="redesign", color=HERO_COLOR)

    for i, (b, a) in enumerate(zip(before_vals, after_vals, strict=False)):
        if b and b > 0 and not np.isnan(b):
            ratio = b / a if a > 0 else float("inf")
            if np.isfinite(ratio):
                colour = GREEN_BRIGHT if ratio >= 1 else RED
                ax.annotate(
                    f"{ratio:.2f}×",
                    (xs[i] + width / 2, a),
                    textcoords="offset points", xytext=(0, 4),
                    ha="center", va="bottom", fontsize=8.5,
                    color=colour, fontweight="600",
                )

    ax.set_yscale("log")
    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Median wall time (s)")
    ax.set_title("Baseline vs redesign", pad=10)
    ax.legend(loc="upper right", fontsize=9)


# ── Figure builders ──────────────────────────────────────────────────────────


def build_throughput_figure(buckets, theme):
    ctx = set_style(theme)
    fig = plt.figure(figsize=(17, 8))
    apply_background(fig, theme)

    dark_bg = dark_bg_for(theme)
    c_latest, c_overlap = line_accents(dark_bg)

    gs = fig.add_gridspec(1, 2, wspace=0.25, top=0.77, bottom=0.13, left=0.06, right=0.97)
    ax_w = fig.add_subplot(gs[0, 0])
    ax_r = fig.add_subplot(gs[0, 1])
    for ax in (ax_w, ax_r):
        ax.set_facecolor("none")
        ax.grid(True, which="both", color=ctx.grid, linewidth=0.5, alpha=0.35)

    plot_write_throughput(ax_w, buckets)
    plot_read_throughput(ax_r, buckets, c_latest, c_overlap)

    overline(fig, 0.935, "Throughput (end-to-end)")
    headline(fig, 0.88, "How", "fast", "is EnergyDB?")
    subtitle(fig, 0.835, "Through the NodeScope fluent API. PG metadata resolution included in every measurement.")
    footer(fig)
    return fig


def build_timing_figure(buckets, theme):
    ctx = set_style(theme)
    fig = plt.figure(figsize=(17, 12))
    apply_background(fig, theme)

    dark_bg = dark_bg_for(theme)
    write_labels = [lbl for lbl, _ in WRITE_PHASES] + ["Overhead"]
    read_labels = [lbl for lbl, _ in READ_PHASES] + ["Overhead"]
    write_colours = phase_palette(write_labels, dark_bg=dark_bg)
    read_colours = phase_palette(read_labels, dark_bg=dark_bg)

    gs = fig.add_gridspec(1, 3, wspace=0.28, top=0.78, bottom=0.22, left=0.06, right=0.97)
    ax_w = fig.add_subplot(gs[0, 0])
    ax_l = fig.add_subplot(gs[0, 1])
    ax_o = fig.add_subplot(gs[0, 2])
    for ax in (ax_w, ax_l, ax_o):
        ax.set_facecolor("none")
        ax.grid(True, axis="y", color=ctx.grid, linewidth=0.5, alpha=0.35)

    _plot_phase_stack(ax_w, buckets, "forecast_write", WRITE_PHASES, write_colours, "Write")
    _plot_phase_stack(ax_l, buckets, "read_latest", READ_PHASES, read_colours, "Read · latest")
    _plot_phase_stack(ax_o, buckets, "read_overlapping_history", READ_PHASES, read_colours, "Read · overlapping")

    # Legend block
    fig.text(0.06, 0.14, "Write phases", fontsize=11, fontweight="bold")
    for i, lbl in enumerate(write_labels):
        fig.text(0.06, 0.12 - i * 0.017, f"● {lbl}", fontsize=9,
                 color=write_colours[lbl])
        fig.text(0.15, 0.12 - i * 0.017, WRITE_PHASE_DESCRIPTIONS.get(lbl, ""),
                 fontsize=8.5, color="#64748B")

    fig.text(0.52, 0.14, "Read phases", fontsize=11, fontweight="bold")
    for i, lbl in enumerate(read_labels):
        fig.text(0.52, 0.12 - i * 0.017, f"● {lbl}", fontsize=9,
                 color=read_colours[lbl])
        fig.text(0.60, 0.12 - i * 0.017, READ_PHASE_DESCRIPTIONS.get(lbl, ""),
                 fontsize=8.5, color="#64748B")

    overline(fig, 0.93, "Where time is spent")
    headline(fig, 0.885, "Per-phase", "timing", "breakdown")
    subtitle(fig, 0.835, "Median phase times (ms) — separates energydb PG ops from timedb CH ops.")
    footer(fig)
    return fig


def build_before_vs_after_figure(before_buckets, after_buckets, theme):
    ctx = set_style(theme)
    fig = plt.figure(figsize=(17, 8))
    apply_background(fig, theme)

    gs = fig.add_gridspec(1, 1, top=0.76, bottom=0.30, left=0.06, right=0.97)
    ax = fig.add_subplot(gs[0, 0])
    ax.set_facecolor("none")
    ax.grid(True, axis="y", color=ctx.grid, linewidth=0.5, alpha=0.35)

    plot_before_vs_after(ax, before_buckets, after_buckets)

    overline(fig, 0.935, "Before vs after")
    headline(fig, 0.88, "Old", "schema", "vs unified events")
    subtitle(fig, 0.835, "Median wall time per (operation, scale). Speedup ratios annotated above the redesign bars.")
    footer(fig)
    return fig


# ── Main ─────────────────────────────────────────────────────────────────────


def _pick_default_csv() -> Path:
    if DEFAULT_CSV.exists():
        return DEFAULT_CSV
    candidates = [p for p in RESULTS_DIR.glob("*.csv") if p.name != "baseline.csv"]
    if not candidates:
        raise FileNotFoundError(f"No CSVs in {RESULTS_DIR}/")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main() -> None:
    after_path = Path(sys.argv[1]) if len(sys.argv) > 1 else _pick_default_csv()
    after_rows = load_rows(after_path)
    after_buckets = by_op_scale(after_rows)

    before_buckets = None
    if BASELINE_CSV.exists():
        before_rows = load_rows(BASELINE_CSV)
        before_buckets = by_op_scale(before_rows)

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Reading after: {after_path}")
    if before_buckets:
        print(f"Reading baseline: {BASELINE_CSV}")
    print(f"Brand style: {'enabled' if _USE_BRAND_STYLE else 'disabled (fallback)'}")

    for style_theme, bg_label in (("dark", "light"), ("light", "dark")):
        fig = build_throughput_figure(after_buckets, style_theme)
        path = PLOTS_DIR / f"throughput_{bg_label}.png"
        fig.savefig(path, dpi=180, facecolor=fig.get_facecolor())
        plt.close(fig)
        print(f"  → {path.relative_to(Path(__file__).parent)}")

        fig = build_timing_figure(after_buckets, style_theme)
        path = PLOTS_DIR / f"timing_breakdown_{bg_label}.png"
        fig.savefig(path, dpi=180, facecolor=fig.get_facecolor())
        plt.close(fig)
        print(f"  → {path.relative_to(Path(__file__).parent)}")

        fig = build_before_vs_after_figure(before_buckets, after_buckets, style_theme)
        path = PLOTS_DIR / f"before_vs_after_{bg_label}.png"
        fig.savefig(path, dpi=180, facecolor=fig.get_facecolor())
        plt.close(fig)
        print(f"  → {path.relative_to(Path(__file__).parent)}")


if __name__ == "__main__":
    main()
