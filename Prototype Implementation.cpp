import pandas as pd
import numpy as np
import os
import random

PRODUCTS_PATH = "products.csv"
TRANSACTIONS_PATH = "transactions.csv"

# Always generate random sample data (for demonstration)
print("🔄 Generating random product and transaction data...")

# Create sample products
categories = ['Lab Equipment', 'Chemicals', 'Protective Gear']
products = []
for i in range(1, 31):
    category = random.choice(categories)
    base_price = round(random.uniform(20, 500), 2)
    cost = round(base_price * random.uniform(0.6, 0.9), 2)
    products.append({
        "sku": f"SKU{i:04d}",
        "name": f"Product{i:04d}",
        "category": category,
        "base_price": base_price,
        "cost": cost
    })
pd.DataFrame(products).to_csv(PRODUCTS_PATH, index=False)

# Create sample transactions
transactions = []
for _ in range(300):
    product = random.choice(products)
    quantity = random.randint(1, 10)
    transactions.append({
        "sku": product["sku"],
        "price_paid": round(product["base_price"] * random.uniform(0.9, 1.1), 2),
        "quantity": quantity
    })
pd.DataFrame(transactions).to_csv(TRANSACTIONS_PATH, index=False)

# Load CSVs
products = pd.read_csv(PRODUCTS_PATH)
transactions = pd.read_csv(TRANSACTIONS_PATH)

# Convert types
transactions["price_paid"] = transactions["price_paid"].astype(float)
transactions["quantity"] = transactions["quantity"].astype(int)

# Aggregate transaction stats
product_stats = transactions.groupby("sku").agg(
    num_sales=("quantity", "sum"),
    avg_price_paid=("price_paid", "mean")
).reset_index()

# Merge with product metadata
df = pd.merge(products, product_stats, on="sku", how="left")
df["base_price"] = df["base_price"].astype(float)
df["cost"] = df["cost"].astype(float)
df["num_sales"] = df["num_sales"].fillna(0)
df["avg_price_paid"] = df["avg_price_paid"].fillna(0)

# Define anchor threshold (top 20% in popularity)
thresh = df["num_sales"].quantile(0.80)
df["is_anchor"] = np.where(df["num_sales"] >= thresh, 1, 0)

# Compute anchor averages per category
anchor_avg = df[df["is_anchor"] == 1].groupby("category")["avg_price_paid"].mean().reset_index()
anchor_avg.rename(columns={"avg_price_paid": "anchor_avg_price"}, inplace=True)

# Merge back and compute suggested price
final = pd.merge(df, anchor_avg, on="category", how="left")
final["suggested_price"] = np.where(
    final["is_anchor"] == 1,
    final["avg_price_paid"],
    final["anchor_avg_price"] * 0.95
)

# Select relevant columns
result = final[["sku", "name", "category", "base_price", "cost", "num_sales", "is_anchor", "suggested_price"]]

# Save as CSV to avoid Parquet engine dependency
os.makedirs("output", exist_ok=True)
result.to_csv("output/pricing_suggestions.csv", index=False)

print("✅ Pricing suggestions saved to output/pricing_suggestions.csv")
