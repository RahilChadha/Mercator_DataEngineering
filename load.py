"""
LOAD LAYER — Toronto Construction Intelligence Pipeline
=========================================================
Loads processed data into DuckDB — a free, embedded analytical database.

Why DuckDB instead of BigQuery/Snowflake?
- Zero cost, zero setup, no API keys
- Full SQL support (window functions, CTEs, geospatial)
- Reads Parquet natively (columnar, fast)
- In a production setting, this would be BigQuery/Snowflake/Redshift

This mirrors Mercator.ai loading enriched data into their 
cloud data warehouse for querying and serving to the frontend.
"""

import duckdb
import os
import glob

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "data", "processed")
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "mercator_toronto.duckdb")


def load_to_warehouse():
    """
    Load all processed Parquet files into DuckDB tables.
    Create views, indexes, and analytical layers.
    """
    print("=" * 60)
    print("LOAD PIPELINE — Loading into DuckDB Warehouse")
    print("=" * 60)
    
    con = duckdb.connect(DB_PATH)
    
    # --------------------------------------------------------
    # 1. CREATE SCHEMA (staging + analytics layers)
    # --------------------------------------------------------
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    
    # --------------------------------------------------------
    # 2. LOAD RAW TABLES FROM PARQUET
    # --------------------------------------------------------
    
    # Permits
    permits_path = os.path.join(PROCESSED_DIR, "permits_clean.parquet")
    if os.path.exists(permits_path):
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.permits AS 
            SELECT * FROM read_parquet('{permits_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM staging.permits").fetchone()[0]
        print(f"  [OK] staging.permits: {count} rows")
    
    # Notices
    notices_path = os.path.join(PROCESSED_DIR, "notices_clean.parquet")
    if os.path.exists(notices_path):
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.notices AS 
            SELECT * FROM read_parquet('{notices_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM staging.notices").fetchone()[0]
        print(f"  [OK] staging.notices: {count} rows")
    
    # Entity matches
    matches_path = os.path.join(PROCESSED_DIR, "entity_matches.parquet")
    if os.path.exists(matches_path):
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.entity_matches AS 
            SELECT * FROM read_parquet('{matches_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM staging.entity_matches").fetchone()[0]
        print(f"  [OK] staging.entity_matches: {count} rows")
    
    # Scored leads
    leads_path = os.path.join(PROCESSED_DIR, "scored_leads.parquet")
    if os.path.exists(leads_path):
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.scored_leads AS 
            SELECT * FROM read_parquet('{leads_path}')
        """)
        count = con.execute("SELECT COUNT(*) FROM staging.scored_leads").fetchone()[0]
        print(f"  [OK] staging.scored_leads: {count} rows")
    
    # --------------------------------------------------------
    # 3. CREATE ANALYTICS VIEWS (dbt-style transformations)
    # --------------------------------------------------------
    print("\n  Creating analytics views...")
    
    # View: Top leads ranked by score
    con.execute("""
        CREATE OR REPLACE VIEW analytics.top_leads AS
        SELECT 
            lead_id,
            source,
            address,
            description,
            project_stage,
            estimated_cost,
            distance_km,
            lead_score,
            priority,
            suggested_action,
            date,
            latitude,
            longitude,
            ROW_NUMBER() OVER (ORDER BY lead_score DESC) as rank
        FROM staging.scored_leads
        WHERE priority IN ('HIGH', 'MEDIUM')
        ORDER BY lead_score DESC
    """)
    print("  [OK] analytics.top_leads")
    
    # View: Project pipeline by stage
    con.execute("""
        CREATE OR REPLACE VIEW analytics.pipeline_by_stage AS
        SELECT 
            project_stage,
            COUNT(*) as project_count,
            ROUND(AVG(lead_score), 1) as avg_score,
            COUNT(*) FILTER (WHERE priority = 'HIGH') as high_priority_count,
            ROUND(AVG(distance_km), 1) as avg_distance_km
        FROM staging.scored_leads
        GROUP BY project_stage
        ORDER BY 
            CASE project_stage 
                WHEN 'Concept' THEN 1
                WHEN 'Application' THEN 2
                WHEN 'Under Review' THEN 3
                WHEN 'Approved' THEN 4
                WHEN 'Construction' THEN 5
                WHEN 'Complete' THEN 6
                ELSE 7
            END
    """)
    print("  [OK] analytics.pipeline_by_stage")
    
    # View: Geographic hotspots (projects clustered by area)
    con.execute("""
        CREATE OR REPLACE VIEW analytics.geographic_hotspots AS
        SELECT 
            ROUND(latitude, 2) as lat_bucket,
            ROUND(longitude, 2) as lon_bucket,
            COUNT(*) as project_count,
            ROUND(AVG(lead_score), 1) as avg_score,
            ROUND(AVG(distance_km), 1) as avg_distance_km,
            ROUND(AVG(estimated_cost), 0) as avg_cost,
            STRING_AGG(DISTINCT project_stage, ', ') as stages_present
        FROM staging.scored_leads
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        GROUP BY ROUND(latitude, 2), ROUND(longitude, 2)
        HAVING COUNT(*) >= 2
        ORDER BY project_count DESC
    """)
    print("  [OK] analytics.geographic_hotspots")
    
    # View: Cost distribution analysis
    con.execute("""
        CREATE OR REPLACE VIEW analytics.cost_analysis AS
        SELECT
            CASE 
                WHEN estimated_cost < 500000 THEN 'Under $500K'
                WHEN estimated_cost < 1000000 THEN '$500K - $1M'
                WHEN estimated_cost < 5000000 THEN '$1M - $5M'
                WHEN estimated_cost < 10000000 THEN '$5M - $10M'
                WHEN estimated_cost >= 10000000 THEN '$10M+'
                ELSE 'Unknown'
            END as cost_bracket,
            COUNT(*) as project_count,
            ROUND(AVG(lead_score), 1) as avg_score,
            ROUND(AVG(distance_km), 1) as avg_distance_km
        FROM staging.scored_leads
        WHERE estimated_cost IS NOT NULL AND estimated_cost > 0
        GROUP BY 1
        ORDER BY MIN(estimated_cost)
    """)
    print("  [OK] analytics.cost_analysis")
    
    # View: Daily lead feed (what new leads appeared today/recently)
    con.execute("""
        CREATE OR REPLACE VIEW analytics.recent_leads AS
        SELECT *
        FROM staging.scored_leads
        WHERE lead_score >= 40
        ORDER BY date DESC, lead_score DESC
        LIMIT 50
    """)
    print("  [OK] analytics.recent_leads")
    
    print(f"\n[OK] Database saved to: {DB_PATH}")
    con.close()


def run_sample_queries():
    """
    Run sample analytical queries to demonstrate the warehouse.
    These are the kinds of queries Mercator's frontend would run.
    """
    print("\n" + "=" * 60)
    print("SAMPLE ANALYTICAL QUERIES")
    print("=" * 60)
    
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Query 1: Top 10 leads for UofT
    print("\n📊 TOP 10 LEADS FOR UOFT:")
    print("-" * 80)
    result = con.execute("""
        SELECT rank, priority, address, project_stage, 
               lead_score, distance_km, 
               COALESCE('$' || CAST(ROUND(estimated_cost/1000000.0, 1) AS VARCHAR) || 'M', 'N/A') as est_value
        FROM analytics.top_leads
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))
    
    # Query 2: Pipeline by stage
    print("\n\n📊 PROJECT PIPELINE BY STAGE:")
    print("-" * 60)
    result = con.execute("SELECT * FROM analytics.pipeline_by_stage").fetchdf()
    print(result.to_string(index=False))
    
    # Query 3: Cost distribution
    print("\n\n📊 COST DISTRIBUTION:")
    print("-" * 60)
    result = con.execute("SELECT * FROM analytics.cost_analysis").fetchdf()
    print(result.to_string(index=False))
    
    # Query 4: Geographic hotspots
    print("\n\n📊 GEOGRAPHIC HOTSPOTS (clustered project areas):")
    print("-" * 60)
    result = con.execute("SELECT * FROM analytics.geographic_hotspots LIMIT 10").fetchdf()
    print(result.to_string(index=False))
    
    con.close()


if __name__ == "__main__":
    load_to_warehouse()
    run_sample_queries()
