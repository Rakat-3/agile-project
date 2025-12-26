# store_contract_data_worker.py
import os
import time
import requests
import psycopg2
from psycopg2 import OperationalError, InterfaceError
from psycopg2.extras import Json
from datetime import datetime, timezone

# ----------------- CAMUNDA CONFIG -----------------
BASE_URL = os.getenv("CAMUNDA_REST", "http://camunda:8080/engine-rest").rstrip("/")
USERNAME = os.getenv("CAMUNDA_USER", "demo")
PASSWORD = os.getenv("CAMUNDA_PASS", "demo")
TOPIC = os.getenv("CAMUNDA_TOPIC", "store-contract-data")
WORKER_ID = os.getenv("WORKER_ID", "python-store-worker")

FETCH_URL = f"{BASE_URL}/external-task/fetchAndLock"

LOCK_DURATION_MS = int(os.getenv("LOCK_DURATION_MS", "600000"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))

# ----------------- DB CONFIG -----------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_TABLE = os.getenv("CONTRACTS_TABLE", "public.contracts_data").strip()

session = requests.Session()
session.auth = (USERNAME, PASSWORD)

_conn = None


def connect_db():
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {DB_TABLE} (
            id TEXT PRIMARY KEY,
            process_instance_id TEXT,
            last_task_id TEXT,
            saved_at_utc TIMESTAMPTZ,

            contract_title TEXT,
            contract_type TEXT,
            budget TEXT,
            deadline TEXT,
            description TEXT,

            raw_variables JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
    conn.commit()


def get_conn():
    global _conn
    if _conn is None or _conn.closed != 0:
        _conn = connect_db()
        ensure_schema(_conn)

    # health check
    try:
        with _conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return _conn
    except Exception:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = connect_db()
        ensure_schema(_conn)
        return _conn


def flatten_vars(d: dict) -> dict:
    out = {}
    for k, v in (d or {}).items():
        out[k] = v.get("value") if isinstance(v, dict) and "value" in v else v
    return out


def get_process_variables(piid: str) -> dict:
    url = f"{BASE_URL}/process-instance/{piid}/variables"
    r = session.get(url, params={"deserializeValues": "false"}, timeout=30)
    r.raise_for_status()
    return flatten_vars(r.json())


def fetch_and_lock():
    payload = {
        "workerId": WORKER_ID,
        "maxTasks": 1,
        "usePriority": True,
        "topics": [{
            "topicName": TOPIC,
            "lockDuration": LOCK_DURATION_MS,
            "variables": ["contractTitle", "contractType", "budget", "deadline", "description"],
        }],
    }
    r = session.post(FETCH_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def complete_task(task_id: str, variables: dict = None):
    url = f"{BASE_URL}/external-task/{task_id}/complete"
    payload = {"workerId": WORKER_ID, "variables": variables or {}}
    r = session.post(url, json=payload, timeout=30)
    r.raise_for_status()


def fail_task(task_id: str, msg: str):
    url = f"{BASE_URL}/external-task/{task_id}/failure"
    payload = {"workerId": WORKER_ID, "errorMessage": str(msg)[:5000], "retries": 3, "retryTimeout": 15000}
    try:
        session.post(url, json=payload, timeout=30)
    except Exception:
        pass


def upsert(conn, piid: str, task_id: str, v: dict):
    sql = f"""
    INSERT INTO {DB_TABLE} (
        id, process_instance_id, last_task_id, saved_at_utc,
        contract_title, contract_type, budget, deadline, description,
        raw_variables, updated_at
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
    ON CONFLICT (id) DO UPDATE SET
        last_task_id=EXCLUDED.last_task_id,
        saved_at_utc=EXCLUDED.saved_at_utc,
        contract_title=EXCLUDED.contract_title,
        contract_type=EXCLUDED.contract_type,
        budget=EXCLUDED.budget,
        deadline=EXCLUDED.deadline,
        description=EXCLUDED.description,
        raw_variables=EXCLUDED.raw_variables,
        updated_at=now();
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            piid, piid, task_id, datetime.now(timezone.utc),
            v.get("contractTitle"), v.get("contractType"), v.get("budget"),
            v.get("deadline"), v.get("description"),
            Json(v)
        ))
    conn.commit()


def main():
    if not DATABASE_URL:
        raise SystemExit("❌ DATABASE_URL missing in .env")

    print(f"✅ store-contract worker started | TOPIC={TOPIC} | TABLE={DB_TABLE}", flush=True)

    while True:
        task_id = None
        try:
            tasks = fetch_and_lock()
            if not tasks:
                time.sleep(POLL_SECONDS)
                continue

            task = tasks[0]
            task_id = task["id"]
            piid = task.get("processInstanceId") or f"task:{task_id}"

            v_task = flatten_vars(task.get("variables", {}))
            v_full = get_process_variables(task["processInstanceId"]) if task.get("processInstanceId") else {}
            v = {**v_full, **v_task}

            conn = get_conn()
            upsert(conn, piid, task_id, v)

            now_iso = datetime.now(timezone.utc).isoformat()
            complete_task(task_id, variables={
                "contractStored": {"value": True, "type": "Boolean"},
                "contractStoredAt": {"value": now_iso, "type": "String"},
            })

            print(f"✅ Stored contract + completed task={task_id}", flush=True)

        except (OperationalError, InterfaceError) as e:
            print(f"❌ DB dropped, reconnecting: {e}", flush=True)
            global _conn
            try:
                if _conn:
                    _conn.close()
            except Exception:
                pass
            _conn = None
            time.sleep(2)

        except Exception as e:
            print(f"❌ Error: {e}", flush=True)
            try:
                if _conn and _conn.closed == 0:
                    _conn.rollback()
            except Exception:
                pass
            if task_id:
                fail_task(task_id, str(e))
            time.sleep(3)


if __name__ == "__main__":
    main()
