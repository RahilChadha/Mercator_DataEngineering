-- ============================================================
-- DBT-STYLE SQL MODELS — Toronto Construction Intelligence
-- ============================================================
-- These SQL models represent what would be dbt models in production.
-- Mercator.ai lists dbt (Data Build Tool) as a required skill.
--
-- In production:
--   models/staging/stg_permits.sql
--   models/staging/stg_notices.sql
--   models/marts/fct_leads.sql
--   models/marts/dim_projects.sql
--
-- Here we define them as DuckDB views/tables.
-- ============================================================


-- ============================================================
-- STAGING: stg_permits
-- Clean, typed, deduplicated permits
-- ============================================================
CREATE OR REPLACE VIEW staging.stg_permits AS
SELECT 
    permit_id,
    permit_type,
    structure_type,
    work_description,
    full_address,
    latitude,
    longitude,
    
    -- Standardize dates
    CAST(application_date AS DATE) AS application_date,
    CAST(issued_date AS DATE) AS issued_date,
    CAST(completed_date AS DATE) AS completed_date,
    
    -- Clean cost
    CASE 
        WHEN estimated_cost > 0 THEN estimated_cost 
        ELSE NULL 
    END AS estimated_cost,
    
    status,
    current_use,
    proposed_use,
    project_stage,
    distance_from_uoft_km,
    data_source,
    
    -- Computed fields
    DATEDIFF('day', application_date, COALESCE(issued_date, CURRENT_DATE)) AS days_in_review,
    CASE 
        WHEN proposed_use != current_use THEN TRUE 
        ELSE FALSE 
    END AS is_use_change

FROM staging.permits
WHERE permit_id IS NOT NULL;


-- ============================================================
-- STAGING: stg_notices  
-- Clean, typed, deduplicated notices
-- ============================================================
CREATE OR REPLACE VIEW staging.stg_notices AS
SELECT
    notice_id,
    title,
    subheading,
    decision_body,
    CAST(notice_date AS DATE) AS notice_date,
    address,
    latitude,
    longitude,
    planning_type,
    project_stage,
    distance_from_uoft_km,
    data_source,
    
    -- Extract signal strength from notice type
    CASE 
        WHEN planning_type ILIKE '%zoning%' THEN 'Strong'
        WHEN planning_type ILIKE '%official plan%' THEN 'Strong'
        WHEN planning_type ILIKE '%subdivision%' THEN 'Strong'
        WHEN planning_type ILIKE '%site plan%' THEN 'Medium'
        WHEN planning_type ILIKE '%variance%' THEN 'Weak'
        ELSE 'Unknown'
    END AS signal_strength

FROM staging.notices
WHERE notice_id IS NOT NULL;


-- ============================================================
-- MART: fct_leads (fact table — the core business table)
-- All scored leads unified from both sources
-- ============================================================
CREATE OR REPLACE VIEW analytics.fct_leads AS
SELECT
    lead_id,
    source,
    address,
    description,
    project_stage,
    estimated_cost,
    distance_km AS distance_from_uoft_km,
    latitude,
    longitude,
    date AS activity_date,
    lead_score,
    priority,
    suggested_action,
    
    -- Scoring breakdown (reverse-engineered for transparency)
    CASE 
        WHEN distance_km <= 1 THEN 30
        WHEN distance_km <= 3 THEN 25
        WHEN distance_km <= 5 THEN 20
        WHEN distance_km <= 10 THEN 12
        ELSE 5
    END AS proximity_points,
    
    CASE 
        WHEN project_stage = 'Concept' THEN 20
        WHEN project_stage = 'Application' THEN 15
        WHEN project_stage = 'Under Review' THEN 10
        WHEN project_stage = 'Approved' THEN 5
        WHEN project_stage = 'Construction' THEN 2
        ELSE 0
    END AS stage_points,
    
    -- Business context
    CASE 
        WHEN estimated_cost >= 10000000 THEN 'Mega Project (>$10M)'
        WHEN estimated_cost >= 5000000 THEN 'Major Project ($5-10M)'
        WHEN estimated_cost >= 1000000 THEN 'Mid-Size ($1-5M)'
        WHEN estimated_cost > 0 THEN 'Small (<$1M)'
        ELSE 'Value Unknown'
    END AS project_size_category

FROM staging.scored_leads;


-- ============================================================
-- MART: dim_project_stages (dimension table)
-- Reference table for project lifecycle stages
-- ============================================================
CREATE OR REPLACE TABLE analytics.dim_project_stages AS
SELECT * FROM (VALUES
    ('Concept',       1, 'Earliest signal. Rezoning/land transfer detected. Maximum influence window.'),
    ('Application',   2, 'Formal application submitted. Design in progress. Good time to engage.'),
    ('Under Review',  3, 'City reviewing plans. Can still influence scope and specifications.'),
    ('Approved',      4, 'Permit issued. Limited influence but can still pursue subcontract/supply.'),
    ('Construction',  5, 'Active construction. Late-stage opportunities only (change orders, supply).'),
    ('Complete',      6, 'Project finished. No active opportunity. Historical data only.')
) AS t(stage_name, stage_order, description);


-- ============================================================
-- ANALYTICS: Weekly lead summary (would power email alerts)
-- ============================================================
CREATE OR REPLACE VIEW analytics.weekly_lead_summary AS
SELECT
    priority,
    COUNT(*) AS total_leads,
    ROUND(AVG(lead_score), 1) AS avg_score,
    COUNT(*) FILTER (WHERE project_stage = 'Concept') AS concept_stage,
    COUNT(*) FILTER (WHERE project_stage = 'Application') AS application_stage,
    COUNT(*) FILTER (WHERE estimated_cost >= 5000000) AS high_value,
    ROUND(AVG(distance_from_uoft_km), 1) AS avg_distance_km,
    MIN(distance_from_uoft_km) AS closest_lead_km
FROM analytics.fct_leads
GROUP BY priority
ORDER BY 
    CASE priority 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        ELSE 3 
    END;


-- ============================================================
-- ANALYTICS: Competitive intelligence  
-- What types of projects are being built near UofT?
-- ============================================================
CREATE OR REPLACE VIEW analytics.market_intelligence AS
SELECT
    COALESCE(project_stage, 'Unknown') AS stage,
    source,
    COUNT(*) AS project_count,
    ROUND(AVG(estimated_cost), 0) AS avg_estimated_cost,
    ROUND(AVG(distance_from_uoft_km), 1) AS avg_distance_km,
    ROUND(AVG(lead_score), 1) AS avg_lead_score
FROM analytics.fct_leads
GROUP BY COALESCE(project_stage, 'Unknown'), source
ORDER BY project_count DESC;
