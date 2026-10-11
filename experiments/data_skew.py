from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import broadcast

from benchmark.runner import run_benchmark

SALT_BUCKETS = 8
HOT_CUSTOMER_ID = 1


def build_baseline(transactions: DataFrame, customers: DataFrame):
    return (
        transactions.join(customers, on="customer_id", how="inner")
        .groupBy("segment")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("segment")
    )


def build_salted(transactions: DataFrame, customers: DataFrame):
    # Spread only the hot customer's transactions across salt buckets.
    salted_transactions = transactions.withColumn(
        "salt",
        F.when(
            F.col("customer_id") == HOT_CUSTOMER_ID,
            F.pmod(F.xxhash64("transaction_id"), F.lit(SALT_BUCKETS)),
        ).otherwise(F.lit(0)),
    )

    # Replicate the hot customer's dimension row once per salt bucket.
    salt_values = (
        transactions.sparkSession.range(SALT_BUCKETS)
        .select(F.col("id").cast("int").alias("salt"))
    )

    hot_customers = (
        customers.filter(F.col("customer_id") == HOT_CUSTOMER_ID)
        .crossJoin(salt_values)
    )

    other_customers = customers.filter(
        F.col("customer_id") != HOT_CUSTOMER_ID
    ).withColumn("salt", F.lit(0))

    salted_customers = hot_customers.unionByName(other_customers)

    return (
        salted_transactions.join(
            salted_customers,
            on=["customer_id", "salt"],
            how="inner",
        )
        .groupBy("segment")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .orderBy("segment")
    )


def collect_result(dataframe):
    return [
        (
            row["segment"],
            row["transaction_count"],
            row["total_amount"],
        )
        for row in dataframe.collect()
    ]


def run(spark, skewed_transactions: DataFrame, customers: DataFrame):
    print("\n" + "=" * 65)
    print("EXPERIMENT 3: DATA SKEW AND SALTING")
    print("=" * 65)

    hot_count = skewed_transactions.filter(
        F.col("customer_id") == HOT_CUSTOMER_ID
    ).count()

    total_count = skewed_transactions.count()
    hot_percentage = 100 * hot_count / total_count

    print(
        f"Hot customer: {HOT_CUSTOMER_ID} | "
        f"Transactions: {hot_count:,}/{total_count:,} "
        f"({hot_percentage:.1f}%)"
    )

    baseline_df = build_baseline(skewed_transactions, customers)
    salted_df = build_salted(skewed_transactions, customers)

    # Validate actual aggregate values, not just row counts.
    baseline_values = collect_result(baseline_df)
    salted_values = collect_result(salted_df)

    if baseline_values != salted_values:
        raise AssertionError(
            "Salting changed the aggregated results.\n"
            f"Baseline: {baseline_values}\nSalted: {salted_values}"
        )

    print("\nBaseline execution plan:")
    baseline_df.explain(mode="formatted")

    print("\nSalted execution plan:")
    salted_df.explain(mode="formatted")

    baseline_result = run_benchmark(
        experiment="data_skew",
        variant="baseline_join",
        action=lambda: len(build_baseline(
            skewed_transactions, customers
        ).collect()),
    )

    salted_result = run_benchmark(
        experiment="data_skew",
        variant=f"salted_{SALT_BUCKETS}_buckets",
        action=lambda: len(build_salted(
            skewed_transactions, customers
        ).collect()),
    )

    print(
        "\nInterpretation: salting can distribute work for a hot key, "
        "but this local dataset may not show a speedup. The key result "
        "is equivalent output and a clear understanding of the technique."
    )

    return [baseline_result, salted_result]