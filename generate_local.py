import csv, random, uuid, os
from datetime import datetime, timedelta

random.seed(42)
os.makedirs('data/raw', exist_ok=True)

CATEGORIES = ["Electronics","Clothing","Books","Home","Sports","Toys","Beauty","Grocery"]
STATUSES   = ["pending","processing","shipped","delivered","cancelled","returned"]
COUNTRIES  = ["US","UK","CA","AU","DE","FR","IN","JP","BR","MX"]
SEGMENTS   = ["Bronze","Silver","Gold","Platinum"]
CHANNELS   = ["web","mobile","api"]

def rand_date(days_back=90):
    d = datetime.now() - timedelta(days=random.randint(0, days_back))
    return d.strftime("%Y-%m-%d %H:%M:%S")

# Customers
customers = []
for i in range(500):
    customers.append({
        "customer_id": str(uuid.uuid4()),
        "name": f"Customer {i+1}",
        "email": f"customer{i+1}@email.com",
        "country": random.choice(COUNTRIES),
        "signup_date": rand_date(365),
        "segment": random.choice(SEGMENTS),
        "city": f"City{i+1}"
    })

with open('data/raw/customers.csv','w',newline='') as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader(); w.writerows(customers)
print(f"customers.csv: {len(customers)} rows")

# Products
products = []
for i in range(200):
    price = round(random.uniform(5, 500), 2)
    products.append({
        "product_id": str(uuid.uuid4()),
        "name": f"Product {i+1}",
        "category": random.choice(CATEGORIES),
        "price": price,
        "cost": round(price * random.uniform(0.3, 0.6), 2),
        "stock_qty": random.randint(0, 300),
        "sku": f"SKU-{i+1:04d}"
    })

with open('data/raw/products.csv','w',newline='') as f:
    w = csv.DictWriter(f, fieldnames=products[0].keys())
    w.writeheader(); w.writerows(products)
print(f"products.csv: {len(products)} rows")

# Orders + Order Items
orders = []
order_items = []
for i in range(5000):
    customer = random.choice(customers)
    order_id = str(uuid.uuid4())
    created  = rand_date(90)
    n_items  = random.randint(1, 5)
    total    = 0
    for j in range(n_items):
        product  = random.choice(products)
        qty      = random.randint(1, 4)
        discount = random.choice([0, 0.05, 0.10])
        line     = round(qty * product["price"] * (1 - discount), 2)
        total   += line
        order_items.append({
            "item_id":    str(uuid.uuid4()),
            "order_id":   order_id,
            "product_id": product["product_id"],
            "quantity":   qty,
            "unit_price": product["price"],
            "discount":   discount,
            "line_total": line,
            "created_at": created
        })
    orders.append({
        "order_id":    order_id,
        "customer_id": customer["customer_id"],
        "status":      random.choice(STATUSES),
        "total":       round(total, 2),
        "currency":    "USD",
        "created_at":  created,
        "updated_at":  created,
        "channel":     random.choice(CHANNELS),
        "promo_code":  f"PROMO{random.randint(1,99)}" if random.random() < 0.2 else ""
    })

with open('data/raw/orders.csv','w',newline='') as f:
    w = csv.DictWriter(f, fieldnames=orders[0].keys())
    w.writeheader(); w.writerows(orders)
print(f"orders.csv: {len(orders)} rows")

with open('data/raw/order_items.csv','w',newline='') as f:
    w = csv.DictWriter(f, fieldnames=order_items[0].keys())
    w.writeheader(); w.writerows(order_items)
print(f"order_items.csv: {len(order_items)} rows")

print("\nAll files generated successfully!")
