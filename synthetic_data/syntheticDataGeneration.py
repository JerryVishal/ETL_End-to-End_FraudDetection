import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker()

# Number of records
NUM_USERS = 5000
NUM_PRODUCTS = 500
NUM_TRANSACTIONS = 10000
NUM_INTERACTIONS = 20000

# ------------------- USERS TABLE -------------------
users = pd.DataFrame({
    "user_id": range(1, NUM_USERS + 1),
    "name": [fake.name() for _ in range(NUM_USERS)],
    "email": [fake.email() for _ in range(NUM_USERS)],
    "location": [fake.city() for _ in range(NUM_USERS)],
    "signup_date": [fake.date_between(start_date="-2y", end_date="today") for _ in range(NUM_USERS)]
})
users.to_csv("synthetic_data/users.csv", index=False)

# ------------------- PRODUCTS TABLE -------------------
categories = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Books", "Beauty", "Toys"]
products = pd.DataFrame({
    "product_id": range(1, NUM_PRODUCTS + 1),
    "name": [fake.word().capitalize() + " " + random.choice(["Phone", "Shirt", "Mixer", "Shoes", "Novel", "Perfume", "Doll"]) for _ in range(NUM_PRODUCTS)],
    "category": [random.choice(categories) for _ in range(NUM_PRODUCTS)],
    "price": np.round(np.random.uniform(5, 500), 2),
    "stock": np.random.randint(10, 200)
})
products.to_csv("synthetic_data/products.csv", index=False)

# ------------------- TRANSACTIONS TABLE -------------------
transactions = pd.DataFrame({
    "transaction_id": [f"TXN{str(i).zfill(6)}" for i in range(NUM_TRANSACTIONS)],
    "user_id": np.random.randint(1, NUM_USERS + 1, size=NUM_TRANSACTIONS),
    "product_id": np.random.randint(1, NUM_PRODUCTS + 1, size=NUM_TRANSACTIONS),
    "amount": np.round(np.random.uniform(5, 500), 2),
    "payment_method": np.random.choice(["Credit Card", "Debit Card", "PayPal", "Crypto", "Bank Transfer"], NUM_TRANSACTIONS),
    "status": np.random.choice(["Success", "Failed", "Pending"], NUM_TRANSACTIONS),
    "timestamp": pd.date_range(start="2024-01-01", periods=NUM_TRANSACTIONS, freq="T")
})
transactions.to_csv("synthetic_data/transactions.csv", index=False)

# ------------------- USER INTERACTIONS TABLE -------------------
actions = ["Viewed", "Added to Cart", "Wishlist", "Purchased"]
user_interactions = pd.DataFrame({
    "interaction_id": range(1, NUM_INTERACTIONS + 1),
    "user_id": np.random.randint(1, NUM_USERS + 1, size=NUM_INTERACTIONS),
    "product_id": np.random.randint(1, NUM_PRODUCTS + 1, size=NUM_INTERACTIONS),
    "action": np.random.choice(actions, NUM_INTERACTIONS),
    "timestamp": pd.date_range(start="2024-01-01", periods=NUM_INTERACTIONS, freq="T")
})
user_interactions.to_csv("synthetic_data/user_interactions.csv", index=False)

# ------------------- PAYMENTS TABLE -------------------
payment_methods = ["Credit Card", "Debit Card", "PayPal", "Crypto", "Bank Transfer"]
card_types = ["Visa", "MasterCard", "Amex", "None"]

payments = pd.DataFrame({
    "transaction_id": transactions["transaction_id"],
    "payment_method": transactions["payment_method"],
    "card_type": [random.choice(card_types) if method in ["Credit Card", "Debit Card"] else "None" for method in transactions["payment_method"]],
    "status": transactions["status"]
})
payments.to_csv("synthetic_data/payments.csv", index=False)

print("Synthetic e-commerce dataset generated successfully! ✅")