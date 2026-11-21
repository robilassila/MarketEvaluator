#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Draws figures from the CSV files that scripts/benchmark.py writes under results/.

    uv run scripts/plot-benchmarks.py          (or: pip install matplotlib; python3 scripts/plot-benchmarks.py)

Writes each figure as PNG to results/figures/. A figure, or a panel of one,
is skipped when its input is missing. Single figures fit one ACM column, two-panel figures the
full page width.

    throughput   events/s against parallelism, mean ± standard deviation over repetitions,
                 against ideal linear scaling from P = 1
    speedup      speedup and parallel efficiency against parallelism
    latency      p50/p95/p99 window latency against the achieved input rate
    latency-cdf  distribution of window latency per input rate
    cpu          mean CPU per container against parallelism and input rate
    memory       peak memory per container against parallelism and input rate
"""

import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullLocator  # noqa: E402

RESULTS = Path(__file__).resolve().parent.parent / "results"
THROUGHPUT = RESULTS / "throughput"
LATENCY = RESULTS / "latency"
FIGURES = RESULTS / "figures"

COLUMN = 3.33
PAGE = 7.0
HEIGHT = 2.3
CONTAINERS = {"kafka": "#4C72B0", "job": "#DD8452", "producer": "#55A868"}
PERCENTILES = {"p50_ms": ("p50", "#4C72B0"), "p95_ms": ("p95", "#DD8452"), "p99_ms": ("p99", "#C44E52")}
REFERENCE = "#7F7F7F"

plt.rcParams.update({
    "font.size": 8, "axes.titlesize": 8, "axes.labelsize": 8, "legend.fontsize": 7,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.3, "savefig.bbox": "tight",
})


def abbreviated(axis, minor: bool = True) -> None:
    """Labels a logarithmic axis as 200, 500, 1k, 2k, … instead of powers of ten.
    Without `minor`, only the major ticks, such as ones set explicitly, are labelled."""
    def label(value, _):
        return f"{value / 1e6:g}M" if value >= 1e6 else f"{value / 1e3:g}k" if value >= 1e3 else f"{value:g}"
    axis.set_major_formatter(FuncFormatter(label))
    if minor:
        axis.set_minor_locator(LogLocator(subs=(2, 5)))
        axis.set_minor_formatter(FuncFormatter(label))
    else:
        axis.set_minor_locator(NullLocator())


def read_csv(path: Path) -> list[dict[str, str]] | None:
    if not path.exists():
        return None
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows or None


def read_host(directory: Path) -> dict | None:
    path = directory / "host.json"
    return json.loads(path.read_text()) if path.exists() else None


def save(figure, name: str) -> None:
    figure.savefig(FIGURES / f"{name}.png", dpi=200)
    plt.close(figure)
    print(f"Wrote {name}")


def throughput_by_parallelism(rows: list[dict[str, str]]) -> dict[int, list[float]]:
    runs = defaultdict(list)
    for row in rows:
        runs[int(row["parallelism"])].append(float(row["events_per_second"]))
    return dict(sorted(runs.items()))


def parallelism_axis(axis, parallelisms: list[int]) -> None:
    axis.set_xscale("log", base=2)
    axis.set_xticks(parallelisms, [str(p) for p in parallelisms])
    axis.minorticks_off()
    axis.set_xlabel("Parallelism P")


def mark_cpus(axis, host: dict | None) -> None:
    if host:
        axis.axvline(host["cpus"], color=REFERENCE, linestyle=":", linewidth=1,
                     label=f"{host['cpus']} CPUs")


def plot_throughput(rows, host) -> None:
    runs = throughput_by_parallelism(rows)
    parallelisms = list(runs)
    means = [statistics.mean(values) for values in runs.values()]
    deviations = [statistics.stdev(values) if len(values) > 1 else 0 for values in runs.values()]
    figure, axis = plt.subplots(figsize=(COLUMN, HEIGHT))
    if parallelisms[0] == 1:
        axis.plot(parallelisms, [means[0] * p for p in parallelisms], color=REFERENCE,
                  linestyle="--", linewidth=1, label="Linear scaling")
    axis.errorbar(parallelisms, means, yerr=deviations, marker="o",
                  markersize=3, capsize=2, color=CONTAINERS["job"], label="Measured")
    mark_cpus(axis, host)
    parallelism_axis(axis, parallelisms)
    axis.set_yscale("log")
    abbreviated(axis.yaxis)
    axis.set_ylabel("Throughput (events/s)")
    axis.legend(frameon=False)
    save(figure, "throughput")


def plot_speedup(rows, host) -> None:
    runs = throughput_by_parallelism(rows)
    if 1 not in runs:
        print("Skipped speedup: no run at parallelism 1")
        return
    parallelisms = list(runs)
    base = statistics.mean(runs[1])
    speedups = [statistics.mean(values) / base for values in runs.values()]
    figure, (speedup, efficiency) = plt.subplots(1, 2, figsize=(PAGE, HEIGHT))
    speedup.plot(parallelisms, parallelisms, color=REFERENCE, linestyle="--", linewidth=1, label="Ideal")
    speedup.plot(parallelisms, speedups, marker="o", markersize=3, color=CONTAINERS["job"], label="Measured")
    speedup.set_ylabel("Speedup over P = 1")
    efficiency.axhline(1, color=REFERENCE, linestyle="--", linewidth=1, label="Ideal")
    efficiency.plot(parallelisms, [s / p for s, p in zip(speedups, parallelisms)], marker="o",
                    markersize=3, color=CONTAINERS["job"], label="Measured")
    efficiency.set_ylabel("Parallel efficiency")
    efficiency.set_ylim(bottom=0)
    for axis in (speedup, efficiency):
        mark_cpus(axis, host)
        parallelism_axis(axis, parallelisms)
        axis.legend(frameon=False)
    speedup.set_yscale("log", base=2)
    speedup.set_yticks(parallelisms, [str(p) for p in parallelisms])
    speedup.minorticks_off()
    save(figure, "speedup")


def plot_latency(rows) -> None:
    rows = sorted(rows, key=lambda row: float(row["achieved_rate"]))
    rates = [float(row["achieved_rate"]) for row in rows]
    figure, axis = plt.subplots(figsize=(COLUMN, HEIGHT))
    for column, (label, color) in PERCENTILES.items():
        axis.plot(rates, [float(row[column]) for row in rows], marker="o", markersize=3,
                  color=color, label=label)
    axis.set_xscale("log")
    axis.set_yscale("log")
    axis.set_xticks(rates)
    abbreviated(axis.xaxis, minor=False)
    abbreviated(axis.yaxis)
    axis.set_xlabel("Achieved input rate (events/s)")
    axis.set_ylabel("Window latency (ms)")
    axis.legend(frameon=False)
    save(figure, "latency")


def plot_latency_cdf(summary) -> None:
    figure, axis = plt.subplots(figsize=(COLUMN, HEIGHT))
    colors = plt.colormaps["viridis"]
    rows = sorted(summary, key=lambda row: int(row["target_rate"]))
    for index, row in enumerate(rows):
        windows = read_csv(LATENCY / f"rate-{row['target_rate']}" / "latency.csv")
        if not windows:
            continue
        values = sorted(max(float(window["latency_millis"]), 1) for window in windows)
        fractions = [(rank + 1) / len(values) for rank in range(len(values))]
        axis.step(values, fractions, where="post", color=colors(0.85 * index / max(1, len(rows) - 1)),
                  label=f"{int(float(row['achieved_rate'])):,} ev/s")
    axis.set_xscale("log")
    axis.set_xlabel("Window latency (ms)")
    axis.set_ylabel("Fraction of windows")
    axis.set_ylim(0, 1.02)
    axis.legend(frameon=False)
    save(figure, "latency-cdf")


def per_run(rows: list[dict[str, str]], keys: tuple[str, ...], value: str, reduce) -> dict:
    """Reduces each run's samples per container, then averages over repetitions:
    {(key values): {container: value}}."""
    samples = defaultdict(list)
    for row in rows:
        samples[(tuple(row[key] for key in keys), row.get("repetition"), row["container"])].append(
            float(row[value]))
    repetitions = defaultdict(list)
    for (key, _, container), values in samples.items():
        repetitions[(key, container)].append(reduce(values))
    result = defaultdict(dict)
    for (key, container), values in repetitions.items():
        result[key][container] = statistics.mean(values)
    return dict(sorted(result.items(), key=lambda item: tuple(int(part) for part in item[0])))


def stacked_bars(axis, by_run: dict, scale: float) -> None:
    labels = [f"{int(key[0]):,}" for key in by_run]
    bottoms = [0.0] * len(labels)
    for container, color in CONTAINERS.items():
        heights = [values.get(container, 0) / scale for values in by_run.values()]
        if not any(heights):
            continue
        axis.bar(labels, heights, bottom=bottoms, color=color, label=container, width=0.6)
        bottoms = [b + h for b, h in zip(bottoms, heights)]


def resource_figure(name: str, value: str, reduce, ylabel: str, scale: float, ceiling) -> None:
    panels = []
    throughput_rows = read_csv(THROUGHPUT / "resources.csv")
    if throughput_rows:
        panels.append((per_run(throughput_rows, ("parallelism",), value, reduce),
                       "Parallelism P (bounded job)", read_host(THROUGHPUT)))
    latency_rows = read_csv(LATENCY / "resources.csv")
    if latency_rows:
        panels.append((per_run(latency_rows, ("target_rate",), value, reduce),
                       "Target input rate (events/s, live job)", read_host(LATENCY)))
    if not panels:
        print(f"Skipped {name}: no resources.csv")
        return
    figure, axes = plt.subplots(1, len(panels), figsize=(COLUMN * len(panels) + 0.3, HEIGHT), squeeze=False)
    for axis, (by_run, xlabel, host) in zip(axes[0], panels):
        stacked_bars(axis, by_run, scale)
        if host:
            limit, label = ceiling(host)
            axis.axhline(limit, color=REFERENCE, linestyle=":", linewidth=1, label=label)
        axis.set_xlabel(xlabel)
        axis.set_ylabel(ylabel)
        axis.grid(axis="x", visible=False)
        axis.legend(frameon=False)
    save(figure, name)


def main() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)

    runs = read_csv(THROUGHPUT / "throughput.csv")
    if runs:
        host = read_host(THROUGHPUT)
        plot_throughput(runs, host)
        plot_speedup(runs, host)
    else:
        print("Skipped throughput and speedup: no throughput.csv")

    summary = read_csv(LATENCY / "summary.csv")
    if summary:
        plot_latency(summary)
        plot_latency_cdf(summary)
    else:
        print("Skipped latency and latency-cdf: no summary.csv")

    resource_figure("cpu", "cpu_percent", statistics.mean, "Mean CPU (% of one CPU)", 1,
                    lambda host: (host["cpus"] * 100, f"{host['cpus']} CPUs"))
    resource_figure("memory", "memory_bytes", max, "Peak memory (GiB)", 2**30,
                    lambda host: (host["memory_bytes"] / 2**30, "Docker memory"))


if __name__ == "__main__":
    main()
