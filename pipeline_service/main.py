from fastapi import FastAPI, HTTPException
from database import Base, engine, SessionLocal
from models import Customer
from ingestion import ingest_data

app = FastAPI()

Base.metadata.create_all(bind=engine)

@app.post("/api/ingest")
def ingest():
    count = ingest_data()
    return {"status": "success", "records_processed": count}

@app.get("/api/customers")
def get_customers(page: int = 1, limit: int = 10):
    db = SessionLocal()
    offset = (page - 1) * limit
    data = db.query(Customer).offset(offset).limit(limit).all()
    total = db.query(Customer).count()

    return {
        "data": data,
        "total": total,
        "page": page,
        "limit": limit
    }

@app.get("/api/customers/{id}")
def get_customer(id: str):
    db = SessionLocal()
    customer = db.get(Customer, id)

    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    return customer