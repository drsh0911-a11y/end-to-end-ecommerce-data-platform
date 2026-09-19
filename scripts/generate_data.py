import os
import json
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Ensure output directory exists
OUTPUT_DIR = "data/source"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Generating e-commerce source datasets with intentional data quality flaws...")

# 1. Customers Dataset (10,000+ records)
num_customers = 10500
customers = []
countries = ["EG", "AE", "SA", "US", "GB", "DE"]
segments = ["Retail", "Wholesale", "VIP", "Standard"]

for i in range(1, num_customers + 1):
    cust_id = f"C{i:05d}"
    if i == 50 or i == 500:
        cust_id = None

    customers.append({
        "customer_id": cust_id,
        "customer_name": fake.name(),
        "email": fake.email(),
        "country": random.choice(countries),
        "city": fake.city(),
        "customer_segment": random.choice(segments),
        "signup_date": fake.date_between(start_date='-2y', end_date='-1y').isoformat(),
        "updated_at": fake.date_time_between(start_date='-1y', end_date='now').isoformat()
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(os.path.join(OUTPUT_DIR, "customers.csv"), index=False)

# 2. Products Dataset (2,000+ records)
num_products = 2200
products = []
categories = ["Electronics", "Apparel", "Home & Kitchen", "Beauty", "Sports"]
brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]

for i in range(1, num_products + 1):
    products.append({
        "product_id": f"P{i:04d}",
        "product_name": fake.word().capitalize() + " " + random.choice(["Pro", "Max", "Lite", "Standard"]),
        "category": random.choice(categories),
        "brand": random.choice(brands),
        "unit_price": round(random.uniform(10.0, 1500.0), 2),
        "created_at": fake.date_between(start_date='-2y', end_date='-1y').isoformat(),
        "updated_at": fake.date_time_between(start_date='-1y', end_date='now').isoformat()
    })

products_df = pd.DataFrame(products)
products_df.to_csv(os.path.join(OUTPUT_DIR, "products.csv"), index=False)
valid_product_ids = products_df["product_id"].tolist()
valid_customer_ids = [c for c in customers_df["customer_id"].dropna().tolist()]

# 3. Orders Dataset (100,000+ records)
num_orders = 100000
orders = []
order_statuses = ["Completed", "Shipped", "Processing", "Cancelled", "Refunded"]
payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Cash on Delivery"]

for i in range(1, num_orders + 1):
    order_id = f"ORD{i:07d}"
    if i == 100 or i == 2000:
        order_id = "ORD0000100"

    orders.append({
        "order_id": order_id,
        "customer_id": random.choice(valid_customer_ids) if random.random() > 0.02 else None,
        "order_timestamp": fake.date_time_between(start_date='-6m', end_date='now').isoformat(),
        "order_status": random.choice(order_statuses),
        "country": random.choice(countries),
        "payment_method": random.choice(payment_methods),
        "updated_at": fake.date_time_between(start_date='-6m', end_date='now').isoformat()
    })

with open(os.path.join(OUTPUT_DIR, "orders.json"), "w") as f:
    json.dump(orders, f, indent=2)

# 4. Order Items Dataset (300,000+ records)
num_items = 300000
order_items = []

for i in range(1, num_items + 1):
    order_index = random.randint(1, num_orders)
    quantity = random.randint(1, 5)
    if i == 75 or i == 5000:
        quantity = -1

    product_id = random.choice(valid_product_ids)
    if i == 300:
        product_id = "P_INVALID_999"

    order_items.append({
        "order_id": f"ORD{order_index:07d}",
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": round(random.uniform(10.0, 500.0), 2),
        "discount": round(random.choice([0.0, 0.05, 0.1, 0.15, 0.2]), 2)
    })

with open(os.path.join(OUTPUT_DIR, "order_items.json"), "w") as f:
    json.dump(order_items, f, indent=2)

# 5. Payments Dataset (100,000+ records)
payments = []
payment_statuses = ["Success", "Failed", "Pending", "Refunded"]

for i in range(1, num_orders + 1):
    amount = round(random.uniform(20.0, 2000.0), 2)
    if i == 450:
        amount = -50.0

    payments.append({
        "payment_id": f"PAY{i:07d}",
        "order_id": f"ORD{i:07d}",
        "payment_timestamp": fake.date_time_between(start_date='-6m', end_date='now').isoformat(),
        "payment_status": random.choice(payment_statuses),
        "payment_amount": amount,
        "payment_method": random.choice(payment_methods)
    })

with open(os.path.join(OUTPUT_DIR, "payments.json"), "w") as f:
    json.dump(payments, f, indent=2)

# 6. User Events Dataset (500,000+ records)
num_events = 500000
events = []
event_types = ["login", "logout", "product_view", "add_to_cart", "purchase", "refund"]
device_types = ["Mobile iOS", "Mobile Android", "Desktop", "Tablet"]

for i in range(1, num_events + 1):
    events.append({
        "event_id": f"EVT{i:09d}",
        "user_id": random.choice(valid_customer_ids),
        "event_timestamp": fake.date_time_between(start_date='-3m', end_date='now').isoformat(),
        "event_type": random.choice(event_types),
        "product_id": random.choice(valid_product_ids) if random.random() > 0.3 else None,
        "session_id": f"SES{random.randint(1, 100000):06d}",
        "country": random.choice(countries),
        "device_type": random.choice(device_types)
    })

with open(os.path.join(OUTPUT_DIR, "events.json"), "w") as f:
    json.dump(events, f, indent=2)

print("Data generation complete! All source files saved in data/source/")
