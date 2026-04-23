"""
EXTRACT LAYER — Toronto Construction Intelligence Pipeline
============================================================
Pulls real data from:
1. City of Toronto Open Data (CKAN API) — Building Permits
2. City of Toronto Public Notices API — Rezoning & Planning Applications
3. City of Toronto Open Data (CKAN API) — Development Applications (with lat/lon)

Mirrors what Mercator.ai does: ingest fragmented municipal data sources.
"""

import requests
import pandas as pd
import json
import os
import time
from pyproj import Transformer

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

# ============================================================
# 1. BUILDING PERMITS — Toronto Open Data (CKAN API)
# ============================================================

def extract_building_permits(limit=5000):
    """
    Extract active building permits from Toronto Open Data.
    Uses the CKAN datastore_search API — no API key needed.
    
    This is analogous to Mercator scraping municipal permit portals.
    """
    print("[EXTRACT] Fetching building permits from Toronto Open Data...")
    
    # Toronto Open Data CKAN base URL
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    
    # First, find the building permits package
    package_url = f"{base_url}/api/3/action/package_show"
    
    # Try active permits dataset
    datasets_to_try = [
        "building-permits-active-permits",
        "building-permits-cleared-permits",
    ]
    
    all_records = []
    
    for dataset_id in datasets_to_try:
        try:
            resp = requests.get(package_url, params={"id": dataset_id}, timeout=30)
            if resp.status_code != 200:
                print(f"  [WARN] Dataset '{dataset_id}' returned {resp.status_code}, skipping...")
                continue
                
            pkg = resp.json()
            if not pkg.get("success"):
                print(f"  [WARN] Dataset '{dataset_id}' not found, skipping...")
                continue
            
            resources = pkg["result"]["resources"]
            print(f"  Found {len(resources)} resources in '{dataset_id}'")
            
            # Find CSV or datastore resources
            for resource in resources:
                res_format = resource.get("format", "").upper()
                res_id = resource["id"]
                res_name = resource.get("name", "unknown")
                
                if resource.get("datastore_active"):
                    # Use datastore API for structured access
                    print(f"  [EXTRACT] Pulling from datastore: {res_name} ({res_id})")
                    
                    offset = 0
                    batch_size = 1000
                    resource_records = []
                    
                    while len(resource_records) < limit:
                        ds_url = f"{base_url}/api/3/action/datastore_search"
                        ds_resp = requests.get(ds_url, params={
                            "id": res_id,
                            "limit": batch_size,
                            "offset": offset
                        }, timeout=30)
                        
                        if ds_resp.status_code != 200:
                            break
                        
                        ds_data = ds_resp.json()
                        if not ds_data.get("success"):
                            break
                        
                        records = ds_data["result"]["records"]
                        if not records:
                            break
                        
                        resource_records.extend(records)
                        offset += batch_size
                        
                        total = ds_data["result"].get("total", 0)
                        print(f"    Fetched {len(resource_records)}/{min(total, limit)} records...")
                        
                        if offset >= total:
                            break
                    
                    if resource_records:
                        all_records.extend(resource_records)
                        print(f"  [OK] Got {len(resource_records)} records from {res_name}")
                    break  # Got data from first datastore resource
                    
                elif res_format == "CSV":
                    # Download CSV directly
                    csv_url = resource.get("url")
                    if csv_url:
                        print(f"  [EXTRACT] Downloading CSV: {res_name}")
                        csv_resp = requests.get(csv_url, timeout=60)
                        if csv_resp.status_code == 200:
                            csv_path = os.path.join(DATA_DIR, f"{dataset_id}.csv")
                            with open(csv_path, "w") as f:
                                f.write(csv_resp.text)
                            df = pd.read_csv(csv_path)
                            all_records.extend(df.head(limit).to_dict("records"))
                            print(f"  [OK] Got {len(df)} records from CSV")
                            break
                            
        except Exception as e:
            print(f"  [ERROR] {dataset_id}: {e}")
            continue
    
    if all_records:
        df = pd.DataFrame(all_records)
        output_path = os.path.join(DATA_DIR, "building_permits_raw.parquet")
        df.to_parquet(output_path, index=False)
        print(f"[OK] Saved {len(df)} building permit records to {output_path}")
        return df
    else:
        print("[WARN] No building permit data retrieved, generating sample data...")
        return _generate_sample_permits()


def _generate_sample_permits():
    """Fallback: generate realistic sample permit data if API is unavailable."""
    import random
    from datetime import datetime, timedelta
    
    print("[FALLBACK] Generating realistic sample building permit data...")
    
    permit_types = ["New Building", "Addition/Alteration", "Demolition", 
                     "Interior Alteration", "Mechanical/Plumbing"]
    statuses = ["Application", "Review", "Permit Issued", "Inspection", "Closed"]
    work_types = ["Commercial", "Residential", "Industrial", "Institutional"]
    
    # Real Toronto addresses near UofT (within 20km radius)
    addresses = [
        ("100 St George St", 43.6590, -79.3999, "M5S 3G3"),
        ("700 University Ave", 43.6548, -79.3897, "M5G 1Z5"),
        ("1 Dundas St W", 43.6561, -79.3802, "M5G 1Z3"),
        ("200 Front St W", 43.6445, -79.3856, "M5V 3K2"),
        ("100 Queens Park", 43.6644, -79.3926, "M5S 2C6"),
        ("2200 Yonge St", 43.7065, -79.3986, "M4S 2C6"),
        ("3300 Bloor St W", 43.6487, -79.5154, "M8X 2X2"),
        ("1900 Eglinton Ave E", 43.7275, -79.3006, "M1L 2L6"),
        ("800 Lawrence Ave W", 43.7214, -79.4429, "M6A 1C2"),
        ("4700 Keele St", 43.7710, -79.5032, "M3J 1P3"),
        ("55 Lake Shore Blvd E", 43.6413, -79.3716, "M5E 1A4"),
        ("401 Bay St", 43.6525, -79.3824, "M5H 2Y4"),
        ("1265 Military Trail", 43.7845, -79.1871, "M1C 1A4"),
        ("5 King St W", 43.6487, -79.3783, "M5H 1A1"),
        ("250 Bloor St E", 43.6702, -79.3777, "M4W 1E6"),
        ("789 Don Mills Rd", 43.7272, -79.3412, "M3C 1T9"),
        ("3401 Dufferin St", 43.7305, -79.4523, "M6A 2T9"),
        ("50 Wellesley St E", 43.6643, -79.3804, "M4Y 1G2"),
        ("150 Borough Dr", 43.7735, -79.2577, "M1P 4N7"),
        ("1530 Birchmount Rd", 43.7558, -79.2731, "M1P 2G5"),
    ]
    
    records = []
    for i in range(200):
        addr = random.choice(addresses)
        apply_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))
        status = random.choice(statuses)
        value = random.choice([
            random.randint(50000, 500000),
            random.randint(500000, 5000000),
            random.randint(5000000, 50000000),
        ])
        
        records.append({
            "PERMIT_NUM": f"24-{random.randint(100000, 999999)}",
            "REVISION_NUM": 0,
            "PERMIT_TYPE": random.choice(permit_types),
            "STRUCTURE_TYPE": random.choice(work_types),
            "WORK": f"{random.choice(['Construct', 'Renovate', 'Demolish', 'Alter'])} "
                    f"{random.choice(['office building', 'retail space', 'condo tower', 'mixed-use development', 'warehouse', 'restaurant', 'medical clinic', 'school addition'])}",
            "STREET_NUM": addr[0].split(" ")[0],
            "STREET_NAME": " ".join(addr[0].split(" ")[1:]),
            "STREET_TYPE": "",
            "STREET_DIRECTION": "",
            "POSTAL": addr[3],
            "GEO_LAT": addr[1] + random.uniform(-0.005, 0.005),
            "GEO_LONG": addr[2] + random.uniform(-0.005, 0.005),
            "APPLICATION_DATE": apply_date.strftime("%Y-%m-%d"),
            "ISSUED_DATE": (apply_date + timedelta(days=random.randint(30, 180))).strftime("%Y-%m-%d") if status != "Application" else None,
            "COMPLETED_DATE": (apply_date + timedelta(days=random.randint(180, 720))).strftime("%Y-%m-%d") if status == "Closed" else None,
            "STATUS": status,
            "CURRENT_USE": random.choice(["Commercial", "Residential", "Industrial", "Vacant Land", "Institutional"]),
            "PROPOSED_USE": random.choice(["Commercial", "Residential", "Mixed Use", "Industrial", "Institutional"]),
            "EST_CONST_COST": value,
            "DWELLING_UNITS_CREATED": random.choice([0, 0, 0, 10, 50, 100, 200]) if "condo" in str(value) else 0,
        })
    
    df = pd.DataFrame(records)
    output_path = os.path.join(DATA_DIR, "building_permits_raw.parquet")
    df.to_parquet(output_path, index=False)
    print(f"[OK] Generated {len(df)} sample permit records")
    return df


# ============================================================
# 2. PUBLIC NOTICES — Rezoning, Planning Applications
# ============================================================

# Browser-like headers to avoid server-side bot blocks
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-CA,en;q=0.9",
}

def extract_public_notices():
    """
    Extract public notices from City of Toronto Notices API.
    Docs: https://secure.toronto.ca/nm/opendata.do
    Search: https://secure.toronto.ca/nm/search.do

    Tries the live API with streaming (server is slow but does respond).
    No fabricated fallback — returns None if unavailable.
    """
    print("\n[EXTRACT] Fetching public notices from Toronto Notices API...")

    # http redirects to https://secure.toronto.ca/nm/notices.json
    urls_to_try = [
        "https://secure.toronto.ca/nm/notices.json",
        "https://secure.toronto.ca/nm/notices.json?category=Planning",
    ]

    session = requests.Session()
    session.headers.update(_HEADERS)

    for url in urls_to_try:
        try:
            print(f"  Trying {url} (streaming, up to 120s)...")
            # Use streaming so we don't timeout waiting for all bytes
            with session.get(url, timeout=120, stream=True) as resp:
                if resp.status_code != 200:
                    print(f"  [WARN] Notices API returned HTTP {resp.status_code}")
                    continue

                # Read content with a per-chunk timeout guard
                chunks = []
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)
                raw = b"".join(chunks)

            payload = json.loads(raw)
            # API returns either a bare list or {"TotalRecordCount":N, "Records":[...]}
            if isinstance(payload, list):
                notices = payload
            elif isinstance(payload, dict) and "Records" in payload:
                notices = payload["Records"]
                print(f"  Total available: {payload.get('TotalRecordCount', '?')}")
            else:
                print("  [WARN] Notices response was empty or unexpected format")
                continue

            if len(notices) == 0:
                print("  [WARN] Notices list was empty")
                continue

            print(f"  [OK] Retrieved {len(notices)} public notices")

            flat_records = []
            for notice in notices:
                record = {
                    "notice_id": notice.get("noticeId"),
                    "title": notice.get("title"),
                    "subheading": notice.get("subheading"),
                    "decision_body": notice.get("decisionBody"),
                    "notice_date": notice.get("noticeDate"),
                    "notice_description": str(notice.get("noticeDescription", ""))[:500],
                    "contact": json.dumps(notice.get("contact", {})),
                    "signed_by": notice.get("signedBy"),
                    "latitude": None,
                    "longitude": None,
                    "address": "",
                    "planning_app_numbers": json.dumps(notice.get("planningApplicationNumbers", [])),
                    "topics": json.dumps(notice.get("topics", [])),
                }

                addresses = notice.get("addressList", [])
                if addresses:
                    first_addr = addresses[0]
                    # Real API uses latitudeCoordinate/longitudeCoordinate/fullAddress
                    record["address"] = (
                        first_addr.get("fullAddress")
                        or first_addr.get("address", "")
                    )
                    record["latitude"] = first_addr.get("latitudeCoordinate") or first_addr.get("latitude")
                    record["longitude"] = first_addr.get("longitudeCoordinate") or first_addr.get("longitude")

                flat_records.append(record)

            df = pd.DataFrame(flat_records)
            output_path = os.path.join(DATA_DIR, "public_notices_raw.parquet")
            df.to_parquet(output_path, index=False)
            print(f"[OK] Saved {len(df)} real public notice records")
            return df

        except Exception as e:
            print(f"  [ERROR] {url}: {e}")
            continue

    print("  [WARN] Toronto Notices API unavailable — no notice data will be loaded.")
    print("         (Server at secure.toronto.ca/nm/notices.json is non-responsive.)")
    return None


# ============================================================
# 3. DEVELOPMENT APPLICATIONS
# Dataset: https://open.toronto.ca/dataset/development-applications/
# Resource ID: 8907d8ed-c515-4ce9-b674-9f8c6eefcf0d
# Coordinates: X/Y in NAD27 / MTM Zone 10 (EPSG:2019) → convert to WGS84
# ============================================================

# Reusable coordinate transformer (NAD27 MTM Zone 10 → WGS84)
_mtm_to_wgs84 = Transformer.from_crs("EPSG:2019", "EPSG:4326", always_xy=True)

def _convert_mtm_to_latlon(x, y):
    """Convert NAD27/MTM Zone 10 (EPSG:2019) X/Y to WGS84 lon/lat."""
    try:
        x_f, y_f = float(x), float(y)
        if x_f == 0 or y_f == 0:
            return None, None
        lon, lat = _mtm_to_wgs84.transform(x_f, y_f)
        # Sanity-check: Toronto bounding box
        if 43.4 <= lat <= 44.0 and -80.0 <= lon <= -78.8:
            return lat, lon
        return None, None
    except Exception:
        return None, None


def extract_development_applications(limit=2000):
    """
    Pull development applications from Toronto Open Data CKAN.
    Dataset: https://open.toronto.ca/dataset/development-applications/

    Converts the X/Y columns (NAD27/MTM Zone 10) to WGS84 latitude/longitude.
    Fields include: APPLICATION_TYPE, ADDRESS, STATUS, DATE_SUBMITTED,
                    DESCRIPTION, WARD_NAME, APPLICATION_URL, X, Y.
    """
    print("\n[EXTRACT] Fetching development applications...")

    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    # Hardcoded datastore-active resource ID for 'development-applications'
    resource_id = "8907d8ed-c515-4ce9-b674-9f8c6eefcf0d"

    try:
        all_records = []
        offset = 0
        batch_size = 1000

        while len(all_records) < limit:
            resp = requests.get(
                f"{base_url}/api/3/action/datastore_search",
                params={"id": resource_id, "limit": batch_size, "offset": offset},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  [WARN] CKAN returned HTTP {resp.status_code}")
                break

            data = resp.json()
            if not data.get("success"):
                print("  [WARN] CKAN datastore_search returned success=false")
                break

            records = data["result"]["records"]
            if not records:
                break

            all_records.extend(records)
            total = data["result"].get("total", 0)
            print(f"    Fetched {len(all_records)}/{min(total, limit)} development application records...")

            offset += batch_size
            if offset >= total:
                break

        if not all_records:
            print("  [WARN] No development application records retrieved")
            return None

        df = pd.DataFrame(all_records)

        # Convert X/Y (NAD27 MTM Zone 10) → WGS84 latitude/longitude
        if "X" in df.columns and "Y" in df.columns:
            print("  [CONVERT] Converting X/Y (EPSG:2019) → lat/lon (WGS84)...")
            coords = df.apply(lambda r: _convert_mtm_to_latlon(r["X"], r["Y"]), axis=1)
            df["latitude"] = coords.apply(lambda c: c[0])
            df["longitude"] = coords.apply(lambda c: c[1])
            geo_count = df["latitude"].notna().sum()
            print(f"  [OK] Geocoded {geo_count}/{len(df)} records successfully")

        output_path = os.path.join(DATA_DIR, "dev_applications_raw.parquet")
        df.to_parquet(output_path, index=False)
        print(f"[OK] Saved {len(df)} development application records")
        return df

    except Exception as e:
        print(f"  [WARN] Could not fetch dev applications: {e}")
        return None


# ============================================================
# MAIN EXTRACTION ORCHESTRATOR
# ============================================================

def run_extraction():
    """Run the full extraction pipeline."""
    print("=" * 60)
    print("EXTRACTION PIPELINE — Toronto Construction Intelligence")
    print("=" * 60)
    
    permits_df = extract_building_permits(limit=2000)
    notices_df = extract_public_notices()
    dev_apps_df = extract_development_applications()
    
    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"  Building Permits:      {len(permits_df) if permits_df is not None else 0} records")
    print(f"  Public Notices:        {len(notices_df) if notices_df is not None else 0} records")
    print(f"  Dev Applications:      {len(dev_apps_df) if dev_apps_df is not None else 0} records")
    
    return permits_df, notices_df, dev_apps_df


if __name__ == "__main__":
    run_extraction()
