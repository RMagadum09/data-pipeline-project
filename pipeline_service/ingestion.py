import requests
from database import SessionLocal
from models import Customer

FLASK_URL = "http://mock-server:5000/api/customers"

def ingest_data():
    db = SessionLocal()
    page = 1
    total = 0

    while True:
        res = requests.get(f"{FLASK_URL}?page={page}&limit=10")
        data = res.json()["data"]

        if not data:
            break

        for c in data:
            existing = db.get(Customer, c["customer_id"])

            if existing:
                for key, value in c.items():
                    setattr(existing, key, value)
            else:
                db.add(Customer(**c))

        db.commit()
        total += len(data)
        page += 1

    return total