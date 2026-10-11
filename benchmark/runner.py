import csv
import statistics
import time
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "results"
RESULTS_FILE = RESULTS_DIR / "benchmark_results.csv"

FIELDS = [
    "experiment",
    "variant",
    "runs",
    "median_seconds",
    "min_seconds",
    "max_seconds",
    "output_rows",
]


def run_benchmark(
    experiment: str,
    variant: str,
    action: Callable[[], int],
    runs: int = 3,
    warmups: int = 1,
) -> dict:
    if runs < 1:
        raise ValueError("runs must be at least 1")
    if warmups < 0:
        raise ValueError("warmups cannot be negative")

    for _ in range(warmups):
        action()

    durations = []
    output_rows = None

    for _ in range(runs):
        start = time.perf_counter()
        current_output_rows = action()
        elapsed = time.perf_counter() - start

        durations.append(elapsed)

        if output_rows is None:
            output_rows = current_output_rows
        elif output_rows != current_output_rows:
            raise AssertionError(
                f"Inconsistent output rows for {experiment}/{variant}"
            )

    result = {
        "experiment": experiment,
        "variant": variant,
        "runs": runs,
        "median_seconds": round(statistics.median(durations), 4),
        "min_seconds": round(min(durations), 4),
        "max_seconds": round(max(durations), 4),
        "output_rows": output_rows,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = RESULTS_FILE.exists() and RESULTS_FILE.stat().st_size > 0

    with RESULTS_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDS)

        if not file_exists:
            writer.writeheader()

        writer.writerow(result)

    print(
        f"{experiment:20} | {variant:20} | "
        f"median={result['median_seconds']:.4f}s | "
        f"rows={result['output_rows']}"
    )

    return result