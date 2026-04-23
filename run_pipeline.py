"""
MAIN PIPELINE RUNNER — Toronto Construction Intelligence
==========================================================
Executes the full ETL pipeline: Extract → Transform → Load → Analyze

This is a mini-Mercator.ai for Toronto, focused on a 20km radius 
around the University of Toronto campus.

Tech stack:
- Python (ETL orchestration)
- SQL / DuckDB (analytical warehouse — stands in for BigQuery)
- Parquet (columnar storage format)
- Geospatial analysis (haversine distance, spatial matching)
- Apache Airflow (DAG blueprint in /dags/)
- dbt-style SQL models (in /sql/)

Run: python run_pipeline.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from extract import run_extraction
from transform import run_transformation
from load import load_to_warehouse, run_sample_queries
import duckdb


def run_sql_models():
    """Execute dbt-style SQL models against the warehouse."""
    print("\n" + "=" * 60)
    print("RUNNING SQL MODELS (dbt-style)")
    print("=" * 60)
    
    db_path = os.path.join(os.path.dirname(__file__), "data", "mercator_toronto.duckdb")
    sql_path = os.path.join(os.path.dirname(__file__), "models.sql")
    
    con = duckdb.connect(db_path)
    
    with open(sql_path, "r") as f:
        sql_content = f.read()
    
    # Split by semicolons and execute each statement
    # Filter out comment-only blocks
    statements = []
    for s in sql_content.split(";"):
        s = s.strip()
        # Remove lines that are only comments
        lines = [l for l in s.split("\n") if l.strip() and not l.strip().startswith("--")]
        if lines:
            statements.append(s)
    
    for i, stmt in enumerate(statements):
        try:
            con.execute(stmt)
            print(f"  [OK] Model {i+1}/{len(statements)} executed")
        except Exception as e:
            print(f"  [WARN] Model {i+1} error: {str(e)[:80]}")
    
    # Run some final analytics
    print("\n" + "=" * 60)
    print("FINAL ANALYTICS — UofT Construction Intelligence Report")
    print("=" * 60)
    
    # Weekly summary
    print("\n📋 WEEKLY LEAD SUMMARY:")
    print("-" * 70)
    try:
        result = con.execute("SELECT * FROM analytics.weekly_lead_summary").fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"  Could not run: {e}")
    
    # Market intelligence
    print("\n\n🏗️  MARKET INTELLIGENCE (project activity near UofT):")
    print("-" * 70)
    try:
        result = con.execute("SELECT * FROM analytics.market_intelligence").fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"  Could not run: {e}")
    
    # Top 5 actionable leads
    print("\n\n🎯 TOP 5 ACTIONABLE LEADS:")
    print("-" * 90)
    try:
        result = con.execute("""
            SELECT 
                lead_id,
                priority,
                address,
                project_stage,
                project_size_category,
                ROUND(distance_from_uoft_km, 1) as dist_km,
                lead_score,
                suggested_action
            FROM analytics.fct_leads
            WHERE priority = 'HIGH'
            ORDER BY lead_score DESC
            LIMIT 5
        """).fetchdf()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"  Could not run: {e}")
    
    con.close()


def main():
    """Execute the full pipeline."""
    print("╔" + "═" * 58 + "╗")
    print("║  TORONTO CONSTRUCTION INTELLIGENCE PIPELINE             ║")
    print("║  Mini-Mercator.ai — UofT Lead Generation                ║")
    print("║                                                         ║")
    print("║  Tech: Python · SQL · DuckDB · Parquet · Geospatial    ║")
    print("║  Data: Toronto Open Data · City Clerk Notices API       ║")
    print("╚" + "═" * 58 + "╝")
    print()
    
    # Step 1: EXTRACT
    permits_df, notices_df, dev_apps_df = run_extraction()
    
    # Step 2: TRANSFORM
    permits_clean, notices_clean, entity_matches, leads_df = run_transformation()
    
    # Step 3: LOAD
    load_to_warehouse()
    
    # Step 4: SQL MODELS + ANALYTICS
    run_sql_models()
    
    # Step 5: SAMPLE QUERIES
    run_sample_queries()
    
    print("\n\n" + "=" * 60)
    print("✅ PIPELINE COMPLETE")
    print("=" * 60)
    print(f"\nOutputs:")
    print(f"  📁 Raw data:      ./data/raw/")
    print(f"  📁 Processed:     ./data/processed/")
    print(f"  🗄️  Database:      ./data/mercator_toronto.duckdb")
    print(f"  📊 SQL Models:    ./sql/models.sql")
    print(f"  🔄 Airflow DAG:   ./dags/construction_intel_dag.py")
    print(f"\nTo query the database interactively:")
    print(f"  python -c \"import duckdb; con = duckdb.connect('data/mercator_toronto.duckdb'); print(con.sql('SELECT * FROM analytics.top_leads LIMIT 10').fetchdf())\"")


if __name__ == "__main__":
    main()
