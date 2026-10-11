from pyspark.sql import DataFrame, functions as F

from benchmark.runner import run_benchmark


def direct_aggregation(transactions: DataFrame):
    return (
        transactions.groupBy("region")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("region")
    )


def two_stage_aggregation(transactions: DataFrame):
    first_stage = (
        transactions.groupBy("region", "transaction_type")
        .agg(
            F.count("*").alias("partial_count"),
            F.sum("amount").alias("partial_amount"),
        )
    )

    return (
        first_stage.groupBy("region")
        .agg(
            F.sum("partial_count").alias("transaction_count"),
            F.round(F.sum("partial_amount"), 2).alias("total_amount"),
        )
        .orderBy("region")
    )


def normalized_result(dataframe):
    return [
        (
            row["region"],
            row["transaction_count"],
            row["total_amount"],
        )
        for row in dataframe.collect()
    ]


def run(spark, transactions: DataFrame):
    print("\n" + "=" * 65)
    print("EXPERIMENT 4: SHUFFLE REDUCTION")
    print("=" * 65)

    direct = direct_aggregation(transactions)
    staged = two_stage_aggregation(transactions)

    direct_values = normalized_result(direct)
    staged_values = normalized_result(staged)

    if direct_values != staged_values:
        raise AssertionError(
            "Aggregation strategies produced different results.\n"
            f"Direct: {direct_values}\nTwo-stage: {staged_values}"
        )

    print("\nDirect aggregation plan:")
    direct.explain(mode="formatted")

    print("\nTwo-stage aggregation plan:")
    staged.explain(mode="formatted")

    direct_result = run_benchmark(
        experiment="shuffle_reduction",
        variant="direct_aggregation",
        action=lambda: len(direct_aggregation(transactions).collect()),
    )

    staged_result = run_benchmark(
        experiment="shuffle_reduction",
        variant="two_stage_aggregation",
        action=lambda: len(two_stage_aggregation(transactions).collect()),
    )

    print(
        "\nInterpretation: the extra aggregation stage is not automatically "
        "faster. Inspect the physical plans and measured timings before "
        "claiming a performance improvement."
    )

    return [direct_result, staged_result]