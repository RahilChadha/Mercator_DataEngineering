"""
Leads for University of Toronto
================================
Standalone Streamlit dashboard.

Run:  streamlit run app.py
"""

import os
import duckdb
import pandas as pd
import folium
import streamlit as st
from streamlit_folium import st_folium

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Leads for University of Toronto",
    page_icon=None,
    layout="wide",
)

# ── Constants ──────────────────────────────────────────────────────────────────
UOFT_LAT = 43.6629
UOFT_LON = -79.3957
DB_PATH  = os.path.join(os.path.dirname(__file__), "data", "mercator_toronto.duckdb")

PRIORITY_COLOR = {"HIGH": "#e63946", "MEDIUM": "#f4a261", "LOW": "#4c4c6d"}

# ── Styling ────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background-color: #080810; }
    section[data-testid="stSidebar"] {
        background-color: #0f0f1a;
        border-right: 1px solid #1e1e3a;
    }

    /* metric cards */
    div[data-testid="metric-container"] {
        background: #0f0f1a;
        border: 1px solid #2a2a4a;
        border-top: 3px solid #7c3aed;
        border-radius: 6px;
        padding: 14px 20px;
    }
    div[data-testid="metric-container"] label {
        color: #9490b5 !important;
        font-size: 0.72rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-weight: 500;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 1.55rem;
        font-weight: 700;
    }

    /* headings */
    h1 { color: #ffffff !important; font-weight: 700; letter-spacing: -0.02em; }
    h2, h3 { color: #e2e0f0 !important; font-weight: 600; }

    /* divider */
    hr { border-color: #1e1e3a !important; margin: 1rem 0; }

    /* sidebar labels */
    .stSidebar label { color: #c4b5fd !important; font-size: 0.8rem; font-weight: 500; }

    /* dataframe */
    .stDataFrame { border-radius: 6px; overflow: hidden; border: 1px solid #2a2a4a; }

    /* caption */
    .stCaption { color: #6b6b9a !important; font-size: 0.75rem; }

    /* scrollbar */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0f0f1a; }
    ::-webkit-scrollbar-thumb { background: #3d3d6b; border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ── Data loader ────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_leads():
    con = duckdb.connect(DB_PATH, read_only=True)
    df  = con.execute("SELECT * FROM staging.scored_leads").fetchdf()
    con.close()
    df["latitude"]    = pd.to_numeric(df["latitude"],    errors="coerce")
    df["longitude"]   = pd.to_numeric(df["longitude"],   errors="coerce")
    df["lead_score"]  = pd.to_numeric(df["lead_score"],  errors="coerce")
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    return df

all_df = load_leads()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")
    st.markdown("---")

    min_score = st.slider("Minimum Lead Score", 0, 100, 59, 1)
    max_dist  = st.slider("Max Distance from UofT (km)", 0.5, 20.0, 8.5, 0.5)

    st.markdown("")
    priority_opts = st.multiselect(
        "Priority",
        ["HIGH", "MEDIUM", "LOW"],
        default=["HIGH", "MEDIUM"],
    )
    source_opts = st.multiselect(
        "Data Source",
        sorted(all_df["source"].dropna().unique().tolist()),
        default=sorted(all_df["source"].dropna().unique().tolist()),
    )

    st.markdown("---")
    st.markdown(
        "<div style='color:#6b6b9a;font-size:0.72rem;line-height:1.6'>"
        "University of Toronto<br>St. George Campus<br>"
        "43.6629 N, 79.3957 W"
        "</div>",
        unsafe_allow_html=True,
    )

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='color:#ffffff;font-size:2rem;font-weight:700;"
    "letter-spacing:-0.02em;margin-bottom:0'>Leads for University of Toronto</h1>",
    unsafe_allow_html=True,
)
st.markdown(
    "<div style='color:#9490b5;font-size:0.85rem;margin-bottom:1.5rem'>"
    "Construction and development intelligence — Toronto Open Data · AI-filtered · Real-time"
    "</div>",
    unsafe_allow_html=True,
)

# ── Apply filters ──────────────────────────────────────────────────────────────
df = all_df.copy()
df = df[df["lead_score"]  >= min_score]
df = df[df["distance_km"] <= max_dist]
df = df[df["priority"].isin(priority_opts)]
df = df[df["source"].isin(source_opts)]
geo_df = df.dropna(subset=["latitude", "longitude"])

# ── Metrics ────────────────────────────────────────────────────────────────────
high_n    = len(df[df["priority"] == "HIGH"])
med_n     = len(df[df["priority"] == "MEDIUM"])
closest   = df["distance_km"].min() if len(df) else float("nan")
avg_score = df["lead_score"].mean()  if len(df) else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Leads",      f"{len(df):,}")
c2.metric("HIGH Priority",    f"{high_n:,}")
c3.metric("MEDIUM Priority",  f"{med_n:,}")
c4.metric("Closest Lead",     f"{closest:.2f} km" if not pd.isna(closest) else "—")
c5.metric("Average Score",    f"{avg_score:.1f} / 100")

st.markdown("")

# ── Map ────────────────────────────────────────────────────────────────────────
st.markdown("### Development Activity Map")
st.markdown(
    "<div style='color:#9490b5;font-size:0.78rem;margin-bottom:0.6rem'>"
    "Circle size reflects lead score. Click any marker for full details. "
    "Purple ring = 5 km radius from UofT."
    "</div>",
    unsafe_allow_html=True,
)

fmap = folium.Map(
    location=[UOFT_LAT, UOFT_LON],
    zoom_start=13,
    tiles="CartoDB dark_matter",
    prefer_canvas=True,
)

# UofT campus pin
folium.Marker(
    location=[UOFT_LAT, UOFT_LON],
    popup=folium.Popup(
        "<div style='font-family:sans-serif;font-size:0.85rem'>"
        "<b>University of Toronto</b><br>St. George Campus</div>",
        max_width=200,
    ),
    tooltip="University of Toronto — St. George Campus",
    icon=folium.Icon(color="purple", icon="university", prefix="fa"),
).add_to(fmap)

# 5 km radius ring
folium.Circle(
    location=[UOFT_LAT, UOFT_LON],
    radius=5_000,
    color="#7c3aed",
    fill=False,
    weight=1.5,
    opacity=0.5,
    tooltip="5 km radius",
).add_to(fmap)

# Lead markers
for _, row in geo_df.iterrows():
    priority = row.get("priority", "LOW")
    color    = PRIORITY_COLOR.get(priority, "#4c4c6d")
    score    = float(row.get("lead_score", 0))
    radius   = 5 + (score / 100) * 13

    popup_html = f"""
    <div style="font-family:'Inter',sans-serif;min-width:260px;max-width:320px">
      <div style="background:{color};color:#fff;
                  padding:7px 12px;border-radius:5px 5px 0 0;
                  font-weight:600;font-size:0.85rem;letter-spacing:0.03em">
        {priority} &nbsp;·&nbsp; Score: {score:.0f} / 100
      </div>
      <div style="padding:12px;background:#0f0f1a;color:#e2e0f0;
                  border:1px solid #2a2a4a;border-top:none;border-radius:0 0 5px 5px">
        <div style="font-weight:600;margin-bottom:8px;font-size:0.88rem">
          {row.get('address', '—')}
        </div>
        <table style="width:100%;font-size:0.79rem;border-collapse:collapse;color:#c4b5fd">
          <tr>
            <td style="padding:2px 0;color:#9490b5;width:90px">Stage</td>
            <td style="color:#ffffff;font-weight:500">{row.get('project_stage','—')}</td>
          </tr>
          <tr>
            <td style="padding:2px 0;color:#9490b5">Source</td>
            <td style="color:#e2e0f0">{row.get('source','—')}</td>
          </tr>
          <tr>
            <td style="padding:2px 0;color:#9490b5">Distance</td>
            <td style="color:#e2e0f0">{row.get('distance_km',0):.2f} km</td>
          </tr>
          <tr>
            <td style="padding:2px 0;color:#9490b5">Date</td>
            <td style="color:#e2e0f0">{str(row.get('date','—'))[:10]}</td>
          </tr>
        </table>
        <div style="margin-top:10px;padding-top:8px;border-top:1px solid #2a2a4a;
                    font-size:0.77rem;color:#c4b5fd;font-style:italic">
          {str(row.get('description',''))[:160]}
        </div>
        <div style="margin-top:8px;padding:6px 8px;background:#1a1a2e;
                    border-left:3px solid #7c3aed;
                    font-size:0.75rem;color:#a78bfa;line-height:1.4">
          {row.get('suggested_action','—')}
        </div>
      </div>
    </div>
    """
    folium.CircleMarker(
        location=[row["latitude"], row["longitude"]],
        radius=radius,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.8,
        weight=1,
        popup=folium.Popup(popup_html, max_width=340),
        tooltip=f"{priority}  |  {str(row.get('address',''))[:50]}  |  Score {score:.0f}",
    ).add_to(fmap)

st_folium(fmap, width="100%", height=560, returned_objects=[])

st.markdown("")

# ── Bottom panels ──────────────────────────────────────────────────────────────
left, right = st.columns([3, 2])

with left:
    st.markdown("### Top Leads")
    display_cols = ["lead_id", "priority", "address", "project_stage",
                    "source", "distance_km", "lead_score", "suggested_action"]
    display = (
        df[df["priority"].isin(["HIGH", "MEDIUM"])]
        .sort_values("lead_score", ascending=False)
        .head(30)[display_cols]
        .rename(columns={
            "lead_id":          "ID",
            "priority":         "Priority",
            "address":          "Address",
            "project_stage":    "Stage",
            "source":           "Source",
            "distance_km":      "Distance (km)",
            "lead_score":       "Score",
            "suggested_action": "Recommended Action",
        })
    )

    def style_priority(val):
        styles = {
            "HIGH":   "background-color:#4a1020;color:#fca5a5;font-weight:600",
            "MEDIUM": "background-color:#3d2a0a;color:#fcd34d",
            "LOW":    "background-color:#111118;color:#6b6b9a",
        }
        return styles.get(val, "")

    st.dataframe(
        display.style.applymap(style_priority, subset=["Priority"]),
        use_container_width=True,
        height=460,
    )

with right:
    st.markdown("### Summary Statistics")

    st.markdown(
        "<div style='color:#9490b5;font-size:0.75rem;text-transform:uppercase;"
        "letter-spacing:0.07em;margin-bottom:4px'>Pipeline by Stage</div>",
        unsafe_allow_html=True,
    )
    stage_df = (
        df.groupby("project_stage")
        .agg(Leads=("lead_id","count"), Avg_Score=("lead_score","mean"))
        .reset_index()
        .sort_values("Avg_Score", ascending=False)
        .rename(columns={"project_stage":"Stage","Avg_Score":"Avg Score"})
    )
    stage_df["Avg Score"] = stage_df["Avg Score"].round(1)
    st.dataframe(stage_df, use_container_width=True, hide_index=True)

    st.markdown("")
    st.markdown(
        "<div style='color:#9490b5;font-size:0.75rem;text-transform:uppercase;"
        "letter-spacing:0.07em;margin-bottom:4px'>Leads by Source</div>",
        unsafe_allow_html=True,
    )
    src_df = (
        df.groupby(["source","priority"])
        .size()
        .reset_index(name="count")
        .pivot(index="source", columns="priority", values="count")
        .fillna(0).astype(int).reset_index()
        .rename(columns={"source":"Source"})
    )
    st.dataframe(src_df, use_container_width=True, hide_index=True)

    if len(geo_df) > 0:
        st.markdown("")
        st.markdown(
            "<div style='color:#9490b5;font-size:0.75rem;text-transform:uppercase;"
            "letter-spacing:0.07em;margin-bottom:4px'>Top Geographic Clusters</div>",
            unsafe_allow_html=True,
        )
        geo_c = geo_df.copy()
        geo_c["lat_b"] = (geo_c["latitude"]  * 100).round() / 100
        geo_c["lon_b"] = (geo_c["longitude"] * 100).round() / 100
        clusters = (
            geo_c.groupby(["lat_b","lon_b"])
            .agg(Leads=("lead_id","count"),
                 Avg_Score=("lead_score","mean"),
                 Avg_Dist=("distance_km","mean"))
            .reset_index()
            .sort_values("Avg_Score", ascending=False)
            .head(8)
            .rename(columns={"lat_b":"Lat","lon_b":"Lon",
                             "Avg_Score":"Avg Score","Avg_Dist":"Avg Dist (km)"})
        )
        clusters["Avg Score"]    = clusters["Avg Score"].round(1)
        clusters["Avg Dist (km)"]= clusters["Avg Dist (km)"].round(2)
        st.dataframe(clusters, use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown(
    "<div style='color:#3d3d6b;font-size:0.72rem;text-align:center'>"
    "Data: Toronto Open Data (Building Permits, Development Applications) · "
    "Toronto Notices API · AI filtering: Google Gemini 2.5 Flash · "
    "Coordinate conversion: pyproj EPSG:2019 to WGS84 · "
    "Built with Python, DuckDB, Streamlit, Folium"
    "</div>",
    unsafe_allow_html=True,
)
