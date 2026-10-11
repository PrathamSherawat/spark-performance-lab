from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from generate_data import main as generate_data
from experiments.partitioning import run as run_partitioning
from experiments.broadcast_join import run as run_broadcast_join
from experiments.data_skew import run as run_data_skew
from experiments.shuffle_reduction import run as run_shuffle_reduction


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("segment", StringType(), False),
    StructField("region", StringType(), False),
])

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("region", StringType(), False),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SparkPerformanceOptimizationLab")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def read_csv(spark, filename, schema):
    path = DATA_DIR / filename

    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(str(path))
    )


def main():
    required_files = [
        DATA_DIR / "customers.csv",
        DATA_DIR / "transactions.csv",
        DATA_DIR / "transactions_skewed.csv",
    ]

    if not all(path.exists() for path in required_files):
        print("One or more datasets are missing. Generating synthetic data...")
        generate_data()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 65)
        print("SPARK PERFORMANCE OPTIMIZATION LAB")
        print("=" * 65)
        print(f"Spark version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        print(
            "Shuffle partitions: "
            f"{spark.conf.get('spark.sql.shuffle.partitions')}"
        )
        print(
            "Adaptive Query Execution: "
            f"{spark.conf.get('spark.sql.adaptive.enabled')}"
        )

        customers = read_csv(
            spark, "customers.csv", CUSTOMER_SCHEMA
        ).cache()

        transactions = read_csv(
            spark, "transactions.csv", TRANSACTION_SCHEMA
        ).cache()

        skewed_transactions = read_csv(
            spark, "transactions_skewed.csv", TRANSACTION_SCHEMA
        ).cache()

        # Materialize the caches before running benchmarks.
        customer_count = customers.count()
        transaction_count = transactions.count()
        skewed_count = skewed_transactions.count()

        print("\nInput datasets loaded:")
        print(f"Customers: {customer_count:,}")
        print(f"Transactions: {transaction_count:,}")
        print(f"Skewed transactions: {skewed_count:,}")

        run_partitioning(spark, transactions)

        run_broadcast_join(spark, transactions, customers)

        run_data_skew(spark, skewed_transactions, customers)

        run_shuffle_reduction(spark, transactions)

        print("\n" + "=" * 65)
        print("ALL EXPERIMENTS COMPLETED")
        print("=" * 65)
        print(
            "Benchmark results: "
            f"{ROOT / 'results' / 'benchmark_results.csv'}"
        )

    finally:
        customers.unpersist() if "customers" in locals() else None
        transactions.unpersist() if "transactions" in locals() else None
        (
            skewed_transactions.unpersist()
            if "skewed_transactions" in locals()
            else None
        )
        spark.stop()


if __name__ == "__main__":
    main()