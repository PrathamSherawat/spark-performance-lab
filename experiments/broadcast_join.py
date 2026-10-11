from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import broadcast

from benchmark.runner import run_benchmark


def run(spark, transactions: DataFrame, customers: DataFrame):
    print("\n" + "=" * 65)
    print("EXPERIMENT 2: BROADCAST JOIN")
    print("=" * 65)

    original_threshold = spark.conf.get(
        "spark.sql.autoBroadcastJoinThreshold"
    )

    # Force a shuffle-based baseline so the comparison is meaningful.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    def build_join(broadcast_dimension: bool):
        dimension = (
            broadcast(customers)
            if broadcast_dimension
            else customers
        )

        return (
            transactions.join(dimension, on="customer_id", how="inner")
            .groupBy("segment")
            .agg(
                F.count("*").alias("transaction_count"),
                F.round(F.sum("amount"), 2).alias("total_amount"),
            )
        )

    try:
        print("\nBaseline execution plan:")
        build_join(False).explain(mode="formatted")

        print("\nBroadcast execution plan:")
        build_join(True).explain(mode="formatted")

        baseline_result = run_benchmark(
            experiment="broadcast_join",
            variant="sort_merge_baseline",
            action=lambda: len(build_join(False).collect()),
        )

        broadcast_result = run_benchmark(
            experiment="broadcast_join",
            variant="broadcast_hash_join",
            action=lambda: len(build_join(True).collect()),
        )

        if baseline_result["output_rows"] != broadcast_result["output_rows"]:
            raise AssertionError("Join strategies produced different row counts")

        print(
            "\nInterpretation: look for SortMergeJoin in the baseline "
            "and BroadcastHashJoin in the broadcast plan. Timing depends "
            "on data size, memory, and local Spark overhead."
        )

        return [baseline_result, broadcast_result]

    finally:
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            original_threshold,
        )