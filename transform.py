"""
TRANSFORM LAYER — Toronto Construction Intelligence Pipeline
=============================================================
Cleans, normalizes, geocodes, and enriches raw data.

Key transformations (mirrors Mercator.ai's core engineering):
1. Schema normalization across disparate sources
2. Geospatial enrichment — geocoding + distance from UofT
3. Project stage classification (rule-based + heuristic)
4. Entity resolution — linking permits to notices for the same address
5. Lead scoring for UofT's business development team
"""

import pandas as pd
import numpy as np
import json
import os
import math
import time
from datetime import datetime
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise EnvironmentError("GEMINI_API_KEY not set. Copy .env.example to .env and add your key.")
genai.configure(api_key=GEMINI_API_KEY)
_gemini = genai.GenerativeModel("gemini-2.5-flash")

RAW_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# UofT St. George Campus coordinates
UOFT_LAT = 43.6629
UOFT_LON = -79.3957
RADIUS_KM = 20


# ============================================================
# GEOSPATIAL UTILITIES
# ============================================================

def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two lat/lon points."""
    R = 6371  # Earth's radius in km
    
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


def filter_within_radius(df, lat_col, lon_col, center_lat, center_lon, radius_km):
    """Filter dataframe to rows within radius_km of center point."""
    df = df.dropna(subset=[lat_col, lon_col]).copy()
    df["distance_from_uoft_km"] = df.apply(
        lambda row: haversine_km(
            row[lat_col], row[lon_col], center_lat, center_lon
        ), axis=1
    )
    return df[df["distance_from_uoft_km"] <= radius_km].copy()


# ============================================================
# 1. TRANSFORM BUILDING PERMITS
# ============================================================

def transform_permits(df):
    """
    Clean and normalize building permits data.
    Standardize columns, parse dates, compute geospatial features.
    """
    print("\n[TRANSFORM] Processing building permits...")
    
    # Standardize column names to lowercase
    df.columns = [c.lower().strip() for c in df.columns]
    
    # Map common column names (Toronto Open Data has specific naming)
    col_mapping = {
        "permit_num": "permit_id",
        "permit_number": "permit_id",
        "application_date": "application_date",
        "issued_date": "issued_date",
        "completed_date": "completed_date",
        "permit_type": "permit_type",
        "structure_type": "structure_type",
        "work": "work_description",
        "description": "work_description",
        "street_num": "street_num",
        "street_name": "street_name",
        "postal": "postal_code",
        "geo_lat": "latitude",
        "geo_long": "longitude",
        "status": "status",
        "current_use": "current_use",
        "proposed_use": "proposed_use",
        "est_const_cost": "estimated_cost",
        "dwelling_units_created": "dwelling_units",
    }
    
    # Apply mapping where columns exist
    rename_map = {}
    for old, new in col_mapping.items():
        if old in df.columns:
            rename_map[old] = new
    df = df.rename(columns=rename_map)

    # Drop duplicate columns (can occur when multiple source cols map to the same target)
    df = df.loc[:, ~df.columns.duplicated()]

    # Parse dates
    for date_col in ["application_date", "issued_date", "completed_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    
    # Clean numeric fields
    if "estimated_cost" in df.columns:
        df["estimated_cost"] = pd.to_numeric(df["estimated_cost"], errors="coerce")
    
    # Clean lat/lon
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Build full address
    addr_parts = []
    for col in ["street_num", "street_name", "street_type", "street_direction"]:
        if col in df.columns:
            addr_parts.append(col)
    
    if addr_parts:
        df["full_address"] = df[addr_parts].fillna("").astype(str).agg(" ".join, axis=1).str.strip()
    elif "full_address" not in df.columns:
        df["full_address"] = "Unknown"
    
    # Filter to 20km radius from UofT
    if "latitude" in df.columns and "longitude" in df.columns:
        pre_filter = len(df)
        df = filter_within_radius(df, "latitude", "longitude", UOFT_LAT, UOFT_LON, RADIUS_KM)
        print(f"  Geospatial filter: {pre_filter} → {len(df)} records (within {RADIUS_KM}km of UofT)")
    
    # Classify project stage
    df["project_stage"] = df.apply(_classify_permit_stage, axis=1)
    
    # Add source tag
    df["data_source"] = "toronto_building_permits"
    df["ingested_at"] = datetime.now().isoformat()
    
    output_path = os.path.join(PROCESSED_DIR, "permits_clean.parquet")
    df.to_parquet(output_path, index=False)
    print(f"[OK] Saved {len(df)} cleaned permit records")
    return df


def _classify_permit_stage(row):
    """
    Rule-based project stage classification.
    Mirrors Mercator's AI that determines project lifecycle stage.
    
    Stages: Concept → Application → Under Review → Approved → Construction → Complete
    """
    status = str(row.get("status", "")).lower()
    has_issued = pd.notna(row.get("issued_date"))
    has_completed = pd.notna(row.get("completed_date"))
    
    if has_completed or status in ["closed", "complete", "cleared"]:
        return "Complete"
    elif status in ["inspection", "inspecting"]:
        return "Construction"
    elif has_issued or status in ["permit issued", "issued"]:
        return "Approved"
    elif status in ["review", "under review", "in review"]:
        return "Under Review"
    elif status in ["application", "submitted"]:
        return "Application"
    else:
        return "Unknown"


# ============================================================
# 2. GEMINI NOTICE CLASSIFIER
# ============================================================

def classify_notices_with_gemini(df, batch_size=20):
    """
    Use Gemini to filter notices down to only those indicating real
    development activity: construction, demolition, rezoning, site plan
    approval, subdivision, severance, or variance.

    Drops: heritage designations (freeze property), street renamings,
    noise permits, procedural/administrative notices.

    Processes in batches of `batch_size` to minimise API calls.
    Results are cached in processed/notices_gemini_classified.parquet
    so re-runs don't re-call the API for already-seen notice IDs.
    """
    cache_path = os.path.join(PROCESSED_DIR, "notices_gemini_classified.parquet")

    # Load existing cache
    if os.path.exists(cache_path):
        cache = pd.read_parquet(cache_path)
        cached_ids = set(cache["notice_id"].astype(str))
    else:
        cache = pd.DataFrame(columns=["notice_id", "gemini_relevant"])
        cached_ids = set()

    to_classify = df[~df["notice_id"].astype(str).isin(cached_ids)].copy()
    print(f"  [GEMINI] {len(to_classify)} new notices to classify ({len(cached_ids)} cached)...")

    new_results = []
    rows = to_classify.to_dict("records")

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        lines = []
        for j, r in enumerate(batch, 1):
            title = str(r.get("title", ""))[:120]
            desc  = str(r.get("notice_description", "") or r.get("title", ""))[:200]
            lines.append(f'{j}. Title: "{title}" | Context: "{desc}"')

        prompt = (
            "You are a real-estate development intelligence classifier.\n"
            "Classify each city notice as RELEVANT or SKIP.\n\n"
            "RELEVANT = notice signals an upcoming physical change to a property:\n"
            "  construction, demolition, rezoning (OPA/ZBA), site plan approval,\n"
            "  draft plan of subdivision, consent to sever, minor variance for\n"
            "  intensification, or any new development application.\n\n"
            "SKIP = no physical change implied:\n"
            "  heritage designation or listing (FREEZES the property),\n"
            "  street / park renaming, noise exemption permit, procedural matter,\n"
            "  award or recognition, public art, permit extension only.\n\n"
            "Notices:\n"
            + "\n".join(lines)
            + "\n\nReply with a JSON array of strings, one per notice in order.\n"
            'Each element must be exactly "RELEVANT" or "SKIP".\n'
            "No explanation, just the JSON array."
        )

        try:
            resp = _gemini.generate_content(prompt)
            text = resp.text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            labels = json.loads(text.strip())
            if len(labels) != len(batch):
                labels = ["RELEVANT"] * len(batch)   # safe fallback
        except Exception as e:
            print(f"    [WARN] Gemini batch {i//batch_size+1} failed: {e} — defaulting RELEVANT")
            labels = ["RELEVANT"] * len(batch)

        for r, label in zip(batch, labels):
            new_results.append({
                "notice_id": str(r["notice_id"]),
                "gemini_relevant": label.strip().upper() == "RELEVANT",
            })

        done = min(i + batch_size, len(rows))
        print(f"    classified {done}/{len(rows)}...", end="\r")
        time.sleep(0.3)   # gentle rate-limit

    print()   # newline after \r progress

    if new_results:
        new_df = pd.DataFrame(new_results)
        cache = pd.concat([cache, new_df], ignore_index=True)
        cache.to_parquet(cache_path, index=False)

    # Merge relevance flag back onto df
    cache["notice_id"] = cache["notice_id"].astype(str)
    df["notice_id"]    = df["notice_id"].astype(str)
    df = df.merge(cache[["notice_id", "gemini_relevant"]], on="notice_id", how="left")
    df["gemini_relevant"] = df["gemini_relevant"].fillna(True)   # unknown → keep

    before = len(df)
    df = df[df["gemini_relevant"]].drop(columns=["gemini_relevant"])
    print(f"  [GEMINI] Kept {len(df)}/{before} notices as development-relevant")
    return df


# ============================================================
# 3. TRANSFORM PUBLIC NOTICES
# ============================================================

def transform_notices(df):
    """
    Clean and enrich public notices.
    Extract planning application types, parse nested fields.
    """
    print("\n[TRANSFORM] Processing public notices...")
    
    df.columns = [c.lower().strip() for c in df.columns]
    
    # Parse notice date (comes as milliseconds timestamp)
    if "notice_date" in df.columns:
        df["notice_date"] = pd.to_datetime(
            pd.to_numeric(df["notice_date"], errors="coerce"), 
            unit="ms", errors="coerce"
        )
    
    # Parse topics JSON to extract planning type
    def extract_topic(topics_json):
        try:
            topics = json.loads(topics_json) if isinstance(topics_json, str) else topics_json
            if isinstance(topics, list) and topics:
                return topics[0].get("level2", topics[0].get("level1", "Other"))
        except:
            pass
        return "Other"
    
    if "topics" in df.columns:
        df["planning_type"] = df["topics"].apply(extract_topic)
    
    # Clean lat/lon
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Filter to 20km radius
    if "latitude" in df.columns and "longitude" in df.columns:
        pre_filter = len(df)
        df = filter_within_radius(df, "latitude", "longitude", UOFT_LAT, UOFT_LON, RADIUS_KM)
        print(f"  Geospatial filter: {pre_filter} → {len(df)} records (within {RADIUS_KM}km of UofT)")

    # AI filter — drop notices with no development intent
    df = classify_notices_with_gemini(df)

    # Classify as early-stage signal
    df["project_stage"] = df.apply(_classify_notice_stage, axis=1)
    
    # Add source tag
    df["data_source"] = "toronto_public_notices"
    df["ingested_at"] = datetime.now().isoformat()
    
    output_path = os.path.join(PROCESSED_DIR, "notices_clean.parquet")
    df.to_parquet(output_path, index=False)
    print(f"[OK] Saved {len(df)} cleaned notice records")
    return df


def _classify_notice_stage(row):
    """Classify notice into project lifecycle stage."""
    planning_type = str(row.get("planning_type", "")).lower()
    title = str(row.get("title", "")).lower()
    
    if "zoning" in planning_type or "zoning" in title:
        return "Concept"  # Earliest signal — rezoning means new development planned
    elif "official plan" in planning_type:
        return "Concept"
    elif "site plan" in planning_type:
        return "Application"  # Site plan means design is progressing
    elif "subdivision" in planning_type:
        return "Concept"
    elif "minor variance" in planning_type:
        return "Application"
    elif "consent" in planning_type:
        return "Application"
    else:
        return "Concept"  # Public notices are generally early signals


# ============================================================
# 3. ENTITY RESOLUTION — Link permits to notices by proximity
# ============================================================

def resolve_entities(permits_df, notices_df):
    """
    Entity resolution: match permits and notices that likely refer to the 
    same project based on geospatial proximity and temporal overlap.
    
    This is the CORE engineering challenge Mercator solves — a rezoning notice
    at "Lot 14, Block 7" and a permit at "250 Bloor St E" might be the same
    project, but described completely differently.
    
    We use spatial proximity (within 100m) as the linking key.
    """
    print("\n[TRANSFORM] Running entity resolution (spatial matching)...")
    
    if permits_df is None or notices_df is None:
        print("  [SKIP] Missing data for entity resolution")
        return None
    
    # Ensure we have lat/lon
    if "latitude" not in permits_df.columns or "longitude" not in permits_df.columns:
        print("  [SKIP] Permits missing lat/lon columns — skipping entity resolution")
        return None
    p = permits_df.dropna(subset=["latitude", "longitude"]).copy()
    n = notices_df.dropna(subset=["latitude", "longitude"]).copy() if "latitude" in notices_df.columns and "longitude" in notices_df.columns else pd.DataFrame()
    
    if len(p) == 0 or len(n) == 0:
        print("  [SKIP] No geocoded data for matching")
        return None
    
    MATCH_THRESHOLD_KM = 0.1  # 100 meters
    
    matches = []
    for _, notice in n.iterrows():
        for _, permit in p.iterrows():
            dist = haversine_km(
                notice["latitude"], notice["longitude"],
                permit["latitude"], permit["longitude"]
            )
            if dist <= MATCH_THRESHOLD_KM:
                matches.append({
                    "notice_id": notice.get("notice_id"),
                    "permit_id": permit.get("permit_id"),
                    "match_distance_m": round(dist * 1000, 1),
                    "notice_title": notice.get("title"),
                    "permit_description": permit.get("work_description"),
                    "notice_date": notice.get("notice_date"),
                    "permit_date": permit.get("application_date"),
                    "latitude": permit["latitude"],
                    "longitude": permit["longitude"],
                    "address": permit.get("full_address", notice.get("address", "")),
                })
    
    if matches:
        matches_df = pd.DataFrame(matches)
        output_path = os.path.join(PROCESSED_DIR, "entity_matches.parquet")
        matches_df.to_parquet(output_path, index=False)
        print(f"[OK] Found {len(matches_df)} entity matches (permit ↔ notice links)")
        return matches_df
    else:
        print("  [INFO] No spatial matches found within threshold")
        return pd.DataFrame()


# ============================================================
# 4. LEAD SCORING — Score projects for UofT's BD team
# ============================================================

def score_leads(permits_df, notices_df):
    """
    Score and rank leads for UofT's business development.
    
    Scoring factors:
    - Proximity to campus (closer = higher priority)
    - Project value (higher = more impactful)
    - Project stage (earlier stages = more opportunity to influence)
    - Type relevance (institutional/education keywords boost score)
    - Recency (newer = more actionable)
    """
    print("\n[TRANSFORM] Scoring leads for UofT...")
    
    all_leads = []
    
    # Process permits as leads
    if permits_df is not None and len(permits_df) > 0:
        for _, row in permits_df.iterrows():
            score = _compute_lead_score(row, "permit")
            all_leads.append({
                "lead_id": f"PERMIT-{row.get('permit_id', '')}",
                "source": "Building Permit",
                "address": row.get("full_address", ""),
                "description": str(row.get("work_description", ""))[:200],
                "project_stage": row.get("project_stage", "Unknown"),
                "estimated_cost": row.get("estimated_cost"),
                "distance_km": round(row.get("distance_from_uoft_km", 99), 2),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "date": str(row.get("application_date", "")),
                "lead_score": score,
                "priority": _score_to_priority(score),
                "suggested_action": _suggest_action(row, score),
            })
    
    # Process notices as leads
    if notices_df is not None and len(notices_df) > 0:
        for _, row in notices_df.iterrows():
            score = _compute_lead_score(row, "notice")
            all_leads.append({
                "lead_id": f"NOTICE-{row.get('notice_id', '')}",
                "source": "Public Notice",
                "address": row.get("address", ""),
                "description": str(row.get("title", ""))[:200],
                "project_stage": row.get("project_stage", "Unknown"),
                "estimated_cost": None,
                "distance_km": round(row.get("distance_from_uoft_km", 99), 2),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "date": str(row.get("notice_date", "")),
                "lead_score": score,
                "priority": _score_to_priority(score),
                "suggested_action": _suggest_action(row, score),
            })
    
    if all_leads:
        leads_df = pd.DataFrame(all_leads)
        leads_df = leads_df.sort_values("lead_score", ascending=False)
        output_path = os.path.join(PROCESSED_DIR, "scored_leads.parquet")
        leads_df.to_parquet(output_path, index=False)
        print(f"[OK] Scored {len(leads_df)} total leads")
        print(f"  HIGH priority: {len(leads_df[leads_df['priority'] == 'HIGH'])}")
        print(f"  MEDIUM priority: {len(leads_df[leads_df['priority'] == 'MEDIUM'])}")
        print(f"  LOW priority: {len(leads_df[leads_df['priority'] == 'LOW'])}")
        return leads_df
    
    return pd.DataFrame()


def _compute_lead_score(row, source_type):
    """
    Compute a 0-100 lead score.
    Equal weights: Proximity 25 | Stage 25 | Relevance 25 | Recency 25
    """
    score = 0

    # 1. PROXIMITY (0-25 pts)
    dist = row.get("distance_from_uoft_km", 20)
    if dist <= 1:
        score += 25
    elif dist <= 3:
        score += 20
    elif dist <= 5:
        score += 14
    elif dist <= 10:
        score += 8
    else:
        score += 2

    # 2. STAGE (0-25 pts) — earlier = more opportunity to influence
    stage = row.get("project_stage", "")
    stage_scores = {
        "Concept":      25,
        "Application":  20,
        "Under Review": 12,
        "Approved":      6,
        "Construction":  2,
        "Complete":      0,
    }
    score += stage_scores.get(stage, 5)

    # 3. KEYWORD RELEVANCE (0-25 pts)
    text = (str(row.get("work_description", "")) + " " +
            str(row.get("title", "")) + " " +
            str(row.get("proposed_use", "")) + " " +
            str(row.get("current_use", ""))).lower()

    high_relevance   = ["institutional", "education", "university", "research",
                        "laboratory", "student", "academic", "school"]
    medium_relevance = ["mixed use", "commercial", "office", "medical",
                        "hospital", "clinic", "community", "residential", "condo"]

    if any(kw in text for kw in high_relevance):
        score += 25
    elif any(kw in text for kw in medium_relevance):
        score += 13
    else:
        score += 4

    # 4. RECENCY (0-25 pts)
    for date_col in ["application_date", "notice_date"]:
        date_val = row.get(date_col)
        if pd.notna(date_val):
            try:
                if isinstance(date_val, str):
                    date_val = pd.to_datetime(date_val)
                days_old = (pd.Timestamp.now() - pd.Timestamp(date_val)).days
                if days_old <= 30:
                    score += 25
                elif days_old <= 90:
                    score += 18
                elif days_old <= 180:
                    score += 10
                else:
                    score += 2
            except:
                score += 5
            break

    return min(score, 100)


def _score_to_priority(score):
    if score >= 65:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    else:
        return "LOW"


def _suggest_action(row, score):
    """Generate suggested next step based on project context."""
    stage = row.get("project_stage", "")
    
    if score >= 65:
        if stage == "Concept":
            return "URGENT: Contact property owner/developer immediately. Project in earliest stages — maximum influence window."
        elif stage == "Application":
            return "HIGH PRIORITY: Reach out to applicant. Review planning documents and identify collaboration opportunities."
        else:
            return "ENGAGE: Contact project stakeholders to explore partnership or supply opportunities."
    elif score >= 40:
        if stage in ["Concept", "Application"]:
            return "MONITOR: Add to watchlist. Reach out within 2 weeks to establish relationship."
        else:
            return "REVIEW: Assess if project aligns with UofT's strategic development plans."
    else:
        return "LOG: Track for market intelligence. No immediate action required."


# ============================================================
# MAIN TRANSFORM ORCHESTRATOR
# ============================================================

def run_transformation():
    """Run the full transformation pipeline."""
    print("=" * 60)
    print("TRANSFORMATION PIPELINE — Toronto Construction Intelligence")
    print("=" * 60)
    
    # Load raw data
    permits_df = None
    notices_df = None
    
    permits_path = os.path.join(RAW_DIR, "building_permits_raw.parquet")
    if os.path.exists(permits_path):
        permits_df = pd.read_parquet(permits_path)
        permits_df = transform_permits(permits_df)
    
    notices_path = os.path.join(RAW_DIR, "public_notices_raw.parquet")
    if os.path.exists(notices_path):
        notices_df = pd.read_parquet(notices_path)
        notices_df = transform_notices(notices_df)
    
    # Entity resolution
    entity_matches = resolve_entities(permits_df, notices_df)
    
    # Lead scoring
    leads_df = score_leads(permits_df, notices_df)
    
    print("\n" + "=" * 60)
    print("TRANSFORMATION COMPLETE")
    print("=" * 60)
    
    return permits_df, notices_df, entity_matches, leads_df


if __name__ == "__main__":
    run_transformation()
