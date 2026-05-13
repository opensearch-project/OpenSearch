import json, random
from datetime import datetime, timedelta

genders = ["MALE", "FEMALE"]
currencies = ["EUR", "USD", "GBP"]
days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
base_date = datetime(2024, 1, 1)

for i in range(1000000):
    print(json.dumps({"index": {"_index": "ecommerce", "_id": i}}))
    gender = random.choice(genders)
    currency = random.choice(currencies)
    day = random.choice(days)
    order_date = base_date + timedelta(days=random.randint(0, 365), hours=random.randint(0, 23), minutes=random.randint(0, 59))
    total_qty = random.randint(1, 10)
    taxful = round(random.uniform(10, 500), 2)
    taxless = round(taxful * random.uniform(0.8, 1.0), 2)
    doc = {
        "customer_gender": gender, "currency": currency, "day_of_week": day,
        "day_of_week_i": days.index(day),
        "order_date": order_date.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "taxful_total_price": taxful, "taxless_total_price": taxless,
        "total_quantity": total_qty, "total_unique_products": random.randint(1, min(total_qty, 5)),
        "type": "order", "user": "user" + str(i)
    }
    print(json.dumps(doc))
