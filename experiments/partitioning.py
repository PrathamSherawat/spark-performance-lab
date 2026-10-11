from pyspark.sql import DataFrame, functions as F

from benchmark.runner import run_benchmark


def run(spark, transactions: DataFrame):
    print("\n" + "=" * 65)
    print("EXPERIMENT 1: PARTITIONING")
    print("=" * 65)

    results = []

    for partition_count in [2, 4, 8, 16]:
        def action(partitions=partition_count):
            prepared = transactions.repartition(
                partitions, "region"
            )

            aggregated = (
                prepared.groupBy("region")
                .agg(
                    F.count("*").alias("transaction_count"),
                    F.round(F.sum("amount"), 2).alias("total_amount"),
                )
            )

            return len(aggregated.collect())

        result = run_benchmark(
            experiment="partitioning",
            variant=f"{partition_count}_partitions",
            action=action,
        )
        results.append(result)

    print("\nExecution plan for the 8-partition aggregation:")
    (
        transactions.repartition(8, "region")
        .groupBy("region")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
        )
        .explain(mode="formatted")
    )

    print(
        "\nInterpretation: compare median timings, but don't assume "
        "more partitions are always faster. This dataset is small, "
        "so scheduling overhead and local-machine noise matter."
    )

    return results