import json
import random
from datetime import datetime, timedelta

genders = ["MALE", "FEMALE"]
currencies = ["EUR", "USD", "GBP"]
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
categories = ["Men's Clothing", "Women's Clothing", "Men's Shoes", "Women's Shoes",
               "Men's Accessories", "Women's Accessories"]
manufacturers = ["Elitelligence", "Oceanavigations", "Pyramidustries", "Champion Arts",
                  "Tigress Enterprises", "Gnomehouse", "Microlutions", "Spherecords"]

base_date = datetime(2024, 1, 1)

for i in range(1000000):
    # Action line
    print(json.dumps({"index": {"_index": "ecommerce", "_id": i}}))

    # Document
    gender = random.choice(genders)
    currency = random.choice(currencies)
    day = random.choice(days)
    order_date = base_date + timedelta(days=random.randint(0, 365),
                                        hours=random.randint(0, 23),
                                        minutes=random.randint(0, 59))
    total_qty = random.randint(1, 10)
    unique_products = random.randint(1, min(total_qty, 5))
    taxful = round(random.uniform(10, 500), 2)
    taxless = round(taxful * random.uniform(0.8, 1.0), 2)

    doc = {
        "customer_gender": gender,
        "currency": currency,
        "day_of_week": day,
        "day_of_week_i": days.index(day),
        "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "order_id": str(100000 + i),
        "taxful_total_price": taxful,
        "taxless_total_price": taxless,
        "total_quantity": total_qty,
        "total_unique_products": unique_products,
        "category": [random.choice(categories)],
        "customer_first_name": f"User{i}",
        "customer_full_name": f"User{i} Test",
        "customer_id": str(i % 500),
        "customer_last_name": "Test",
        "customer_phone": "",
        "email": f"user{i}@test.zzz",
        "type": "order",
        "user": f"user{i}",
        "manufacturer": [random.choice(manufacturers)],
        "sku": [f"ZO{random.randint(100000, 999999)}"],
    }
    print(json.dumps(doc))
