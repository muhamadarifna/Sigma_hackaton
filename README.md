# Hackton Snowflake SIGMA

<img width="1631" height="453" alt="architecture_solution_v5 drawio (1)" src="https://github.com/user-attachments/assets/b6a47a67-689a-4ed9-8752-1f171e4abea1" />

> End-to-end Snowflake demo/hackathon project: ingestion → enrichment → analytics → automation (Tasks) and **SIGMA Intelligent Apps** (RAG & Cortex Analyst).

## 🏗️ High‑Level Architecture

- **RAW Layer (TELCO.RAW)**: landing zone for ingested data (external/R scripts/synthetic).
- **DATAMART Layer (TELCO.DATAMART)**: normalized facts & dimensions for analytics.
- **Stored Procedures (SP)**:
  - `SP_CUSTOMER_STATUS_PREDICTION`: loads model from stage (`@TELCO.RAW.ML_MODEL`) to predict churn status (Yes/No).
  - `SP_CHURN_PREDICTION`: materializes `TB_F_CHURN_PREDICTION` and generates short churn reasons via **Cortex COMPLETE**.
  - `SP_CUSTOMER_SEGMENTATION`: feature engineering + PCA + KMeans; outputs `TB_F_CUSTOMER_CLUSTER`.
  - `SP_ENRICH_REVIEWS`: review enrichment pipeline (imports code from `@TELCO.APPS.ST_CODE`).
- **Tasks (TK_)**: orchestrate SPs via `SCHEDULE` (cron, Asia/Jakarta) or `AFTER` (chaining)—e.g., churn 08:00, sentiment 09:00.
- **Reporting Marts (`TB_RPT_*`)**: curated tables for dashboards/BI.
- **Intelligent Apps**:
  - **RAG UI**: Streamlit app `inttelegent/sigma.py`.
  - **Cortex Analyst**: `inttelegent/telco_datamart.yml` for NL-to-SQL analytics on `TELCO.DATAMART`.

---

## 📦 Repository Structure
```
├─ Sigma_hackaton-main/
│  ├─ DATAMART/
│  │  ├─ PROCEDURE/
│  │  │  ├─ SP_CHURN_PREDICTION.sql
│  │  │  ├─ SP_CUSTOMER_SEGMENTATION.sql
│  │  │  ├─ SP_CUSTOMER_STATUS_PREDICTION.sql
│  │  │  ├─ SP_ENRICH_REVIEWS.sql
│  │  │  ├─ SP_REFRESH_GENERAL_MARTS_DWH_TELCO.sql
│  │  │  ├─ SP_REFRESH_REVIEW_MARTS_DWH_TELCO.sql
│  │  ├─ TASK/
│  │  │  ├─ TK_CHURN_STEP1_SYNC_R.sql
│  │  │  ├─ TK_CHURN_STEP2_PREDICT.sql
│  │  │  ├─ TK_CHURN_STEP3_SEGMENT.sql
│  │  │  ├─ TK_SENT_STEP1_REFRESH_SYNTH.sql
│  │  │  ├─ TK_SENT_STEP2_SYNC_R_REVIEW.sql
│  │  │  ├─ TK_SENT_STEP3_ENRICH_FULL.sql
│  │  │  ├─ TK_SENT_STEP4_REFRESH_MARTS.sql
│  │  ├─ CHURN ANALYSIS.ipynb
│  │  ├─ CUSTOMER SEGMENTATION.ipynb
│  │  ├─ enrich_reviews.py
│  ├─ FIVETRAN/
│  │  ├─ configuration.json
│  │  ├─ connector.py
│  │  ├─ requirements.txt
│  ├─ INTELEGENT/
│  │  ├─ setup_cortex_analyze.sql
│  │  ├─ setup_rag_cortex_search.sql
│  │  ├─ sigma.py
│  │  ├─ telco_datamart.yml
```

## 🧭 Quick Map
- **DB**: Snowflake
- **Procedure (SP)**: Stored procedures to **update/enrich data** and downstream marts.
- **TASK**: Snowflake Tasks to **run/chain** procedures on schedules.
- **SQL Scripts**: DDL/DML for tables, views, reports.
- **Python**: Snowpark / utilities for modeling and data processing.
- **Docs**: Notes and setup guides.

## 🤖 `inttelegent/` — SIGMA Intelligent Apps

### `sigma.py` — Streamlit UI for RAG
Frontend for **Retrieval Augmented Generation** to query domain knowledge (marts, reports, technical notes).

**Features**
- Context-aware Q&A with citations.
- Search across indexed content; telco focus (churn, sentiment, ops).
- Can integrate with **Cortex Search Service** for enterprise retrieval.

**Run locally**
```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install streamlit langchain snowflake-snowpark-python
streamlit run inttelegent/sigma.py --server.port 8501
```

**Config (typical)**
- Snowflake credentials (account, user, role, warehouse, db, schema).
- LLM API key (if using external provider) or **Snowflake Cortex** as LLM backend.
- Cortex Search Service name (if used) + grants.

### `telco_datamart.yml` — Cortex Analyst Config
Defines an **Analyst agent** for `TELCO.DATAMART` (NL→SQL).

- Persona, guardrails (row limits, no DDL), and warehouse/role.
- Semantic layer hints (table/column aliases) to improve intent accuracy.
- Prompt templates for common telco analyses (churn, sentiment, revenue).

**High-level usage**
1. Review `connection`, `llm`, and `semantic` sections.
2. Register/deploy as per **Snowflake Cortex Analyst** docs.
3. Query with natural language and validate generated SQL.

## 🚀 Getting Started (Snowflake)

1. **Prereqs**
   - Snowflake role with necessary privileges.
   - Active warehouse (e.g., `COMPUTE_WH`).
   - Required stages for models/code: `@TELCO.RAW.ML_MODEL`, `@TELCO.APPS.ST_CODE`.

2. **Deploy SQL Objects**
   - Create schemas/tables/views/procedures per files in `sql/` or `procedures/` (adjust to your repo layout).

3. **Create & Chain Tasks**
   - Use `SCHEDULE` for cron or `AFTER` for DAG chains (ensure tasks in same schema if chained).

4. **Enable & Run**
```sql
ALTER TASK TELCO.JOB.TK_CHURN_STEP1_SYNC_R RESUME;
EXECUTE TASK TELCO.JOB.TK_CHURN_STEP1_SYNC_R;  -- optional manual run
```

## 🧪 Validation & Ops
- Task inventory: `SHOW TASKS IN SCHEMA TELCO.JOB;`
- Task history: `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC;`
- Data checks: validate outputs in `TB_F_*` and `TB_RPT_*`.

## 🔐 Security & Governance
- Separate roles for RAW, DATAMART, and JOB schemas; apply **least privilege**.
- Use stages for artifact distribution; restrict stage access.
- Consider masking policies for sensitive fields.

## 🧰 Troubleshooting
- **Different schema in task chain** → keep chained tasks in the same schema (e.g., `TELCO.JOB`).
- **Warehouse not found** → set `WAREHOUSE=COMPUTE_WH` (or your WH).
- **Procedure import errors** → verify stage paths and grants.

---

_This README is auto-generated from the uploaded repository structure. Adjust paths/names as your project evolves._
