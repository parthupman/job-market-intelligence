# 🏭 Job Market Intelligence Pipeline
## ⚡ Production-Grade End-to-End Data Analytics System

Build an automated job market intelligence system that extracts job postings daily from multiple APIs, tracks salary trends, analyzes demand by skill, and provides a real-time dashboard for job seekers.

### 🏗 Architecture

- **Data Extraction**: Custom Python scripts pulling from Adzuna, RemoteOK, and The Muse APIs.
- **Orchestration**: Apache Airflow DAGs scheduling daily extractions at 6:00 AM UTC.
- **Data Warehouse**: PostgreSQL database.
- **Transformation**: dbt (Data Build Tool) staging, intermediate, and fact/aggregation models.
- **Data Quality**: Great Expectations statistical tests.
- **Visualization**: Streamlit / Metabase Dashboard.
- **Deployment**: Docker containerization & GitHub Actions CI/CD.

### 🚀 Quick Start

1. **Clone repository**
```bash
git clone https://github.com/PARTHUPMAN/job-market-intelligence.git
cd job-market-intelligence
```

2. **Environment Variables**
```bash
cp .env.example .env
# Fill in your database and API credentials
```

3. **Start the Infrastructure**
```bash
# This starts Postgres, Airflow, and Metabase
docker-compose up -d
```

4. **Run pipelines manually**
```bash
# 1. Run Python Extractor
python extract/run_extraction.py

# 2. Run dbt Transformations
cd dbt_project
dbt deps
dbt run
dbt test

# 3. Open the Streamlit Dashboard
cd dashboard/streamlit
pip install -r requirements.txt
streamlit run app.py
```

### 📁 Project Structure highlights
- `airflow/dags/`: Pipeline orchestration logic.
- `dbt_project/`: SQL transformation models.
- `extract/`: Raw API integration scripts.
- `quality/`: Great Expectations test suites.
- `dashboard/`: Streamlit UI.
