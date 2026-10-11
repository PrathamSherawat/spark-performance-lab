# Spark Performance Optimization Lab

A standalone PySpark project for learning and benchmarking common Spark
performance optimization techniques using reproducible synthetic data.

## Objectives

- Compare partition counts during aggregation.
- Compare sort-merge joins with explicit broadcast joins.
- Demonstrate data skew and key salting.
- Compare direct and two-stage aggregations.
- Inspect Spark physical execution plans.
- Benchmark equivalent transformations and save timings to CSV.

## Technology

- Python 3.12
- Apache Spark / PySpark 3.5.8
- Spark SQL
- CSV datasets
- VS Code

## Project layout

- `main.py`: runs the complete experiment suite.
- `generate_data.py`: creates deterministic synthetic datasets.
- `experiments/`: contains four performance experiments.
- `benchmark/runner.py`: measures elapsed times and writes CSV results.
- `data/`: contains generated input data.
- `results/`: contains benchmark output.

## Input data

The data generator creates:

- `customers.csv`: 5,000 customers.
- `transactions.csv`: 50,000 transactions.
- `transactions_skewed.csv`: 50,000 transactions with approximately
  70% of rows assigned to one hot customer.

All data is synthetic. No real financial or personal data is used.

## Experiments

### 1. Partitioning

Tests 2, 4, 8, and 16 partitions and compares the aggregation time.

### 2. Broadcast joins

Compares a sort-merge join baseline with an explicit broadcast hash join.

### 3. Data skew

Compares a normal join with a salted join that spreads a hot key across
eight salt buckets. The results are validated for equivalence.

### 4. Shuffle reduction

Compares a direct region-level aggregation with a two-stage aggregation.
Both strategies must return identical results.

## Running in VS Code

1. Open this project folder in VS Code.
2. Select the Python interpreter from this project's `.venv`.
3. Open `main.py`.
4. Select **Run and Debug** and choose **Run Spark Performance Lab**.
5. Review the execution plans and console output.
6. Open `results/benchmark_results.csv` to inspect benchmark measurements.

The application generates missing input CSV files automatically.

## Interpreting the results

The benchmark runner performs one warm-up followed by three measured runs.
It records the median, minimum, and maximum duration for each variant.

Local timings are sensitive to JVM startup, caching, garbage collection,
available CPU cores, memory, and the machine's workload. Small datasets may
not show a speedup even when an optimization is useful at larger scale.

Do not claim a performance improvement solely because one run is faster.
Compare repeated timings, confirm output equivalence, and inspect the
physical execution plan.

## Limitations

This is a local learning and benchmarking lab, not a distributed production
cluster. Results demonstrate Spark execution concepts but do not guarantee
the same performance on a cluster.
