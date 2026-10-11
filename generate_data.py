import csv
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"

CUSTOMER_COUNT = 5_000
TRANSACTION_COUNT = 50_000
SKEWED_TRANSACTION_COUNT = 50_000
RANDOM_SEED = 42


def write_csv(path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_customers(rng):
    segments = ["Retail", "Business", "Premium", "Enterprise"]
    regions = ["North", "South", "East", "West", "Central"]

    rows = []
    for customer_id in range(1, CUSTOMER_COUNT + 1):
        rows.append({
            "customer_id": customer_id,
            "segment": rng.choice(segments),
            "region": rng.choice(regions),
        })

    return rows


def generate_transactions(rng):
    transaction_types = ["Payment", "Refund", "Transfer", "Purchase"]
    regions = ["North", "South", "East", "West", "Central"]

    rows = []
    for transaction_id in range(1, TRANSACTION_COUNT + 1):
        rows.append({
            "transaction_id": transaction_id,
            "customer_id": rng.randint(1, CUSTOMER_COUNT),
            "amount": round(rng.uniform(10, 10_000), 2),
            "transaction_type": rng.choice(transaction_types),
            "region": rng.choice(regions),
        })

    return rows


def generate_skewed_transactions(rng):
    transaction_types = ["Payment", "Refund", "Transfer", "Purchase"]
    regions = ["North", "South", "East", "West", "Central"]

    rows = []
    for transaction_id in range(1, SKEWED_TRANSACTION_COUNT + 1):
        # Approximately 70% of transactions belong to one hot customer.
        if rng.random() < 0.70:
            customer_id = 1
        else:
            customer_id = rng.randint(2, CUSTOMER_COUNT)

        rows.append({
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "amount": round(rng.uniform(10, 10_000), 2),
            "transaction_type": rng.choice(transaction_types),
            "region": rng.choice(regions),
        })

    return rows


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(RANDOM_SEED)

    customers = generate_customers(rng)
    transactions = generate_transactions(rng)
    skewed_transactions = generate_skewed_transactions(rng)

    write_csv(
        DATA_DIR / "customers.csv",
        ["customer_id", "segment", "region"],
        customers,
    )
    write_csv(
        DATA_DIR / "transactions.csv",
        [
            "transaction_id",
            "customer_id",
            "amount",
            "transaction_type",
            "region",
        ],
        transactions,
    )
    write_csv(
        DATA_DIR / "transactions_skewed.csv",
        [
            "transaction_id",
            "customer_id",
            "amount",
            "transaction_type",
            "region",
        ],
        skewed_transactions,
    )

    print("Synthetic datasets generated successfully.")
    print(f"Customers: {len(customers):,}")
    print(f"Transactions: {len(transactions):,}")
    print(f"Skewed transactions: {len(skewed_transactions):,}")
    print(f"Data directory: {DATA_DIR}")


if __name__ == "__main__":
    main()