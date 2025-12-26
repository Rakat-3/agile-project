# email_worker.py
import os
import time
import requests
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone

# ---------------- CONFIG ----------------
BASE_URL = os.getenv("CAMUNDA_REST", "http://camunda:8080/engine-rest").rstrip("/")
USERNAME = os.getenv("CAMUNDA_USER", "demo")
PASSWORD = os.getenv("CAMUNDA_PASS", "demo")

# MUST match BPMN External Task topic: camunda:topic="send-email"
TOPIC = os.getenv("CAMUNDA_TOPIC", "send-email")
WORKER_ID = os.getenv("WORKER_ID", "python-email-worker")

LOCK_DURATION_MS = int(os.getenv("LOCK_DURATION_MS", "60000"))   # 60s
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))

LEGAL_EMAIL_TO = os.getenv("LEGAL_EMAIL_TO", "legal@example.com")
EMAIL_FROM = os.getenv("EMAIL_FROM", "noreply@example.com")

SMTP_HOST = os.getenv("SMTP_HOST")            # optional
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

AUTH = (USERNAME, PASSWORD)
FETCH_URL = f"{BASE_URL}/external-task/fetchAndLock"

session = requests.Session()
session.auth = AUTH


# ---------------- HELPERS ----------------
def flatten_vars(var_dict: dict) -> dict:
    """Camunda returns {"x":{"value":...,"type":...}} -> we extract values."""
    out = {}
    for k, v in (var_dict or {}).items():
        if isinstance(v, dict) and "value" in v:
            out[k] = v.get("value")
        else:
            out[k] = v
    return out


def send_email(subject: str, body: str):
    """If SMTP_HOST is not set -> print simulated email."""
    if not SMTP_HOST:
        print("\n=== EMAIL (SIMULATED) ===")
        print("TO:", LEGAL_EMAIL_TO)
        print("SUBJECT:", subject)
        print(body)
        print("=========================\n")
        return

    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = LEGAL_EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

    print(f"✅ Email sent to {LEGAL_EMAIL_TO}")


def fetch_and_lock():
    payload = {
        "workerId": WORKER_ID,
        "maxTasks": 1,
        "usePriority": True,
        "topics": [
            {
                "topicName": TOPIC,
                "lockDuration": LOCK_DURATION_MS,
                # You can list variables to fetch; leaving empty also works.
                "variables": [
                    "contractTitle", "contractType", "budget", "deadline", "description",
                    "publishdate", "offerdeadline",
                    "select", "remarks",
                    "offerprice", "offersummary", "offerquality", "offerdecision"
                ],
            }
        ],
    }
    r = session.post(FETCH_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def complete_task(task_id: str, extra_vars: dict):
    url = f"{BASE_URL}/external-task/{task_id}/complete"
    payload = {"workerId": WORKER_ID, "variables": extra_vars}
    r = session.post(url, json=payload, timeout=30)
    r.raise_for_status()


def fail_task(task_id: str, msg: str, retries: int = 1, retry_timeout_ms: int = 5000):
    url = f"{BASE_URL}/external-task/{task_id}/failure"
    payload = {
        "workerId": WORKER_ID,
        "errorMessage": str(msg)[:5000],
        "retries": retries,
        "retryTimeout": retry_timeout_ms,
    }
    session.post(url, json=payload, timeout=30)


# ---------------- MAIN LOOP ----------------
def main():
    print("✅ Email worker started")
    print("BASE_URL:", BASE_URL)
    print("TOPIC:   ", TOPIC)
    print("WORKER:  ", WORKER_ID)
    print("Press Ctrl+C to stop.\n")

    while True:
        task_id = None
        try:
            tasks = fetch_and_lock()
            if not tasks:
                time.sleep(POLL_SECONDS)
                continue

            task = tasks[0]
            task_id = task["id"]
            piid = task.get("processInstanceId")
            variables = flatten_vars(task.get("variables", {}))

            # Debug info (helps you see if offerdecision exists)
            print(f"🔎 Locked task={task_id} PI={piid} keys={sorted(list(variables.keys()))}")

            contract_title = variables.get("contractTitle", "N/A")
            contract_type = variables.get("contractType", "N/A")
            budget = variables.get("budget", "N/A")
            deadline = variables.get("deadline", "N/A")
            description = variables.get("description", "N/A")

            provider = variables.get("select", "N/A")
            offer_price = variables.get("offerprice", "N/A")
            offer_quality = variables.get("offerquality", "N/A")
            offer_decision = variables.get("offerdecision", "N/A")
            offer_summary = variables.get("offersummary", "N/A")

            subject = f"[Contract] Legal review needed: {contract_title}"
            body = (
                "Hello Legal Team,\n\n"
                "A contract requires your review.\n\n"
                f"Contract Title: {contract_title}\n"
                f"Contract Type:  {contract_type}\n"
                f"Budget (€):     {budget}\n"
                f"Deadline:       {deadline}\n"
                f"Description:    {description}\n\n"
                f"Provider:       {provider}\n"
                f"Offer Price:    {offer_price}\n"
                f"Offer Quality:  {offer_quality}\n"
                f"Offer Decision: {offer_decision}\n"
                f"Offer Summary:  {offer_summary}\n\n"
                f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}\n"
            )

            send_email(subject, body)

            now_iso = datetime.now(timezone.utc).isoformat()
            complete_task(
                task_id,
                extra_vars={
                    "emailSent": {"value": True, "type": "Boolean"},
                    "emailSentAt": {"value": now_iso, "type": "String"},
                    "emailTo": {"value": LEGAL_EMAIL_TO, "type": "String"},
                },
            )

            print(f"✅ Completed task={task_id}\n")

        except KeyboardInterrupt:
            print("\nStopping email worker...")
            break
        except Exception as e:
            print("❌ Error:", e)
            if task_id:
                try:
                    fail_task(task_id, f"Email worker failed: {e}", retries=1, retry_timeout_ms=5000)
                except Exception:
                    pass
            time.sleep(2)


if __name__ == "__main__":
    main()
