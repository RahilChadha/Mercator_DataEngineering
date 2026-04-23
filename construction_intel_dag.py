"""
AIRFLOW DAG — Toronto Construction Intelligence Pipeline
==========================================================
This DAG definition shows how the ETL pipeline would be orchestrated 
in production using Apache Airflow (one of Mercator's listed tech skills).

In a real deployment:
- This DAG runs daily on a schedule
- Each task is idempotent and retryable
- Data quality checks gate downstream tasks
- Alerts fire on failure

Since we can't run Airflow in this environment, this file serves as
the orchestration blueprint. The actual execution is in run_pipeline.py.

MERCATOR RELEVANCE:
- Mercator lists Apache Airflow as a required skill
- Their pipeline likely runs on a schedule: poll permit portals → 
  transform → load → serve to users
- This DAG mirrors that architecture
"""

# ============================================================
# AIRFLOW DAG DEFINITION (production-ready blueprint)
# ============================================================

DAG_CONFIG = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'rahil-chadha',
    'depends_on_past': False,
    'email': ['rahil.chadha@mail.utoronto.ca'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='toronto_construction_intelligence',
    default_args=default_args,
    description='ETL pipeline for Toronto construction lead generation',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['construction', 'etl', 'geospatial', 'mercator'],
) as dag:

    start = EmptyOperator(task_id='start')

    # ── EXTRACT LAYER ─────────────────────────────
    extract_permits = PythonOperator(
        task_id='extract_building_permits',
        python_callable=extract_building_permits,
        doc='Pull active building permits from Toronto Open Data CKAN API',
    )

    extract_notices = PythonOperator(
        task_id='extract_public_notices',
        python_callable=extract_public_notices,
        doc='Pull public notices (rezoning, planning) from City Clerk API',
    )

    extract_dev_apps = PythonOperator(
        task_id='extract_development_applications',
        python_callable=extract_development_applications,
        doc='Pull development applications from Toronto Open Data',
    )

    # ── DATA QUALITY CHECKS ───────────────────────
    check_permits = PythonOperator(
        task_id='check_permits_quality',
        python_callable=check_data_quality,
        op_kwargs={'table': 'permits', 'min_rows': 10},
        doc='Validate extracted permits have expected row count and schema',
    )

    check_notices = PythonOperator(
        task_id='check_notices_quality',
        python_callable=check_data_quality,
        op_kwargs={'table': 'notices', 'min_rows': 5},
    )

    # ── TRANSFORM LAYER ───────────────────────────
    transform_permits = PythonOperator(
        task_id='transform_permits',
        python_callable=transform_permits,
        doc='Clean, normalize, geocode, classify project stage',
    )

    transform_notices = PythonOperator(
        task_id='transform_notices',
        python_callable=transform_notices,
        doc='Parse notices, extract planning types, geocode',
    )

    entity_resolution = PythonOperator(
        task_id='entity_resolution',
        python_callable=resolve_entities,
        doc='Spatial matching: link permits to notices within 100m',
    )

    score_leads = PythonOperator(
        task_id='score_leads',
        python_callable=score_leads,
        doc='Compute lead scores based on proximity, value, stage, relevance',
    )

    # ── LOAD LAYER ────────────────────────────────
    load_warehouse = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
        doc='Load processed data into DuckDB (production: BigQuery/Snowflake)',
    )

    # ── SERVE ─────────────────────────────────────
    generate_report = PythonOperator(
        task_id='generate_lead_report',
        python_callable=generate_dashboard,
        doc='Generate interactive dashboard for BD team',
    )

    end = EmptyOperator(task_id='end')

    # ── DAG DEPENDENCIES ──────────────────────────
    # Extract in parallel, then transform, then load
    start >> [extract_permits, extract_notices, extract_dev_apps]
    
    extract_permits >> check_permits >> transform_permits
    extract_notices >> check_notices >> transform_notices
    extract_dev_apps >> transform_notices  # feed into notices if available
    
    [transform_permits, transform_notices] >> entity_resolution
    [transform_permits, transform_notices] >> score_leads
    
    [entity_resolution, score_leads] >> load_warehouse
    load_warehouse >> generate_report >> end
"""

# ============================================================
# DAG VISUALIZATION (ASCII art for README)
# ============================================================

DAG_DIAGRAM = """
                            ┌──────────────────┐
                            │      START       │
                            └────────┬─────────┘
                                     │
                   ┌─────────────────┼─────────────────┐
                   │                 │                  │
                   ▼                 ▼                  ▼
          ┌────────────────┐ ┌──────────────┐ ┌────────────────┐
          │ Extract Permits│ │Extract Notices│ │Extract Dev Apps│
          │ (CKAN API)     │ │ (Clerk API)  │ │ (CKAN API)     │
          └───────┬────────┘ └──────┬───────┘ └────────┬───────┘
                  │                 │                   │
                  ▼                 ▼                   │
          ┌────────────────┐ ┌──────────────┐          │
          │  Quality Check │ │Quality Check │          │
          └───────┬────────┘ └──────┬───────┘          │
                  │                 │                   │
                  ▼                 ▼                   │
          ┌────────────────┐ ┌──────────────────┐      │
          │Transform Permits│ │Transform Notices │◄─────┘
          │ • Normalize     │ │ • Parse topics   │
          │ • Geocode       │ │ • Classify stage │
          │ • Classify stage│ │ • Geocode        │
          └───────┬────────┘ └──────┬───────────┘
                  │                 │
                  ├─────────────────┤
                  │                 │
                  ▼                 ▼
         ┌────────────────┐ ┌──────────────┐
         │Entity Resolution│ │ Score Leads  │
         │(Spatial Match)  │ │ (0-100)      │
         └───────┬────────┘ └──────┬───────┘
                  │                 │
                  └────────┬────────┘
                           │
                           ▼
                  ┌────────────────┐
                  │  Load to DuckDB │
                  │  (Warehouse)    │
                  └───────┬────────┘
                          │
                          ▼
                  ┌────────────────┐
                  │  Dashboard /   │
                  │  Lead Report   │
                  └───────┬────────┘
                          │
                          ▼
                  ┌────────────────┐
                  │      END       │
                  └────────────────┘
"""

if __name__ == "__main__":
    print("AIRFLOW DAG — Toronto Construction Intelligence Pipeline")
    print("=" * 60)
    print(DAG_DIAGRAM)
    print("\nNote: This is the DAG blueprint. Run `python run_pipeline.py`")
    print("to execute the pipeline locally.")
