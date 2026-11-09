
import csv
import random
from datetime import date, timedelta
from pathlib import Path

rng = random.Random(42)

OUTPUT_DIR = Path("sample_data")
OUTPUT_DIR.mkdir(exist_ok=True)

START_DATE = date(2026, 10, 1)
NUM_DAYS = 40
NUM_ORDERS = 800

dates = [
    START_DATE + timedelta(days=i)
    for i in range(NUM_DAYS)
]

# Ensure every reporting date has at least one order.
order_dates = dates.copy()

# Add the remaining orders with some daily variation.
weights = [
    1.25 if d.weekday() >= 5 else 1.0
    for d in dates
]

order_dates += rng.choices(
    dates,
    weights=weights,
    k=NUM_ORDERS - NUM_DAYS
)

rng.shuffle(order_dates)

orders = []

for i, order_day in enumerate(order_dates):
    order_id = str(20001 + i)

    customer_id = f"C{rng.randint(100, 299)}"

    country = rng.choices(
        ["US", "UK", "CA", "CN"],
        weights=[50, 20, 20, 10],
        k=1
    )[0]

    # Right-skewed order values with occasional larger purchases.
    amount = round(
        max(10, min(rng.lognormvariate(4.5, 0.65), 600)),
        2
    )

    orders.append({
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": f"{amount:.2f}",
        "order_date": order_day.isoformat(),
        "country": country
    })

fields = [
    "order_id", "customer_id", "amount",
    "order_date", "country"
]

# Split into three files to test Glue event batching.
for batch_no in range(3):
    batch = orders[batch_no::3]

    filename = OUTPUT_DIR / f"synthetic_orders_batch_{batch_no + 1}.csv"

    with open(filename, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(batch)

    print(f"Created {filename}: {len(batch)} orders")

print(f"Total generated orders: {len(orders)}")
print(f"Unique order IDs: {len(set(o['order_id'] for o in orders))}")
