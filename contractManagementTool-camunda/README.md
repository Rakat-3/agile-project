# Contract Management Tool - camunda
Using camunda, we are implementing a tool to manage contract efficiently and smoothly.


## PROJECT PHASES:

Phase 1 – Base setup (Camunda + PostgreSQL via Docker)

Phase 2: create backend (FastAPI) and connect to PostgreSQL

Phase 3: create the first BPMN process and deploy to Camunda

Phase 4: connect backend ↔ Camunda (start process, complete tasks)

Phase 5: implement archive step


## EXPLANATIONS AND IMPLEMENTS:


Phase 1 – Base setup (Camunda + PostgreSQL via Docker)
------------------------------------------------------
**a. Workstations:**
  
```
contractManagementTool-camunda/
                       ├── backend/  
                       ├── bpmn/  
                       ├── docker/
```

**b. Create docker-compose.yml Inside contract-tool/docker/** 

This will give you:

Camunda 7 at http://localhost:8080

PostgreSQL at localhost:5432 with DB contract_db

**c. Start the containers**

```
cd ~\contract-tool\docker
```

```
docker compose up -d
```

**Wait until containers are up, then check:**

Open browser: 

```
http://localhost:8080
```


**d. Quick check of Camunda**


On the Camunda page:

Open Cockpit → default login usually

user: ```demo```

pass: ```demo```






## Next Phases

Phase 2: create backend (FastAPI) and connect to PostgreSQL

a. Create virtual environment

Your terminal should show:

(venv) user@kali:~/contract-tool/backend$

```
backend/
 ├── main.py          # FastAPI application
 ├── db.py            # Database models + session
```

Phase 3: create the first BPMN process and deploy to Camunda

Phase 4: connect backend ↔ Camunda (start process, complete tasks)

Phase 5: implement archive step
