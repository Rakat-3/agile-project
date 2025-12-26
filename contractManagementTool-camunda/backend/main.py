from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Backend is running!"}
    
    
from db import get_connection

@app.get("/test-db")
def test_db():
    try:
        conn = get_connection()
        conn.close()
        return {"status": "ok", "message": "Database connected"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
        
import requests

CAMUNDA_URL = "http://localhost:8080/engine-rest"

@app.post("/start-process")
def start_process(data: dict):
    """
    Starts the Camunda process and passes initial variables.
    """
    payload = {
        "variables": {
            "contractTitle": {"value": data.get("contractTitle"), "type": "String"},
            "requestedBy": {"value": data.get("requestedBy"), "type": "String"},
        }
    }
    res = requests.post(
        f"{CAMUNDA_URL}/process-definition/key/contract_demo/start",
        json=payload
    )

    return {"camunda_response": res.json()}

