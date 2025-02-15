# Built by Hamza Ahmad - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# Full Streamlit dashboard: dark military-intel theme, 5 pages, 3D globe, live feed

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
AUTO_REFRESH_SECONDS = 60

SEVERITY_COLORS = {
    "CRITICAL": [220, 20, 60, 220],    # Crimson
    "HIGH": [255, 100, 0, 200],         # Orange-red
    "MEDIUM": [255, 200, 0, 180],       # Amber
    "LOW": [30, 144, 255, 160],         # Dodger blue
}

SEVERITY_HEX = {
    "CRITICAL": "#DC143C",
    "HIGH": "#FF6400",
    "MEDIUM": "#FFC800",
    "LOW": "#1E90FF",
}

SEVERITY_RADIUS = {
    "CRITICAL": 120000,
    "HIGH": 80000,
    "MEDIUM": 50000,
    "LOW": 25000,
}

EVENT_TYPE_ICONS = {
    "AIRSTRIKE": "✈",
    "MISSILE_ATTACK": "🚀",
    "GROUND_OPERATION": "⚔",
    "NAVAL": "⚓",
    "CYBER": "💻",
    "PROTEST": "✊",
    "CEASEFIRE": "🕊",
    "DIPLOMATIC": "🤝",
    "SANCTIONS": "🔒",
    "UNKNOWN": "❓",
}

COUNTRY_FLAGS = {
    "Iran": "🇮🇷", "Israel": "🇮🇱", "Gaza": "🇵🇸", "Palestine": "🇵🇸",
    "Lebanon": "🇱🇧", "Syria": "🇸🇾", "Iraq": "🇮🇶", "Yemen": "🇾🇪",
    "Saudi Arabia": "🇸🇦", "Jordan": "🇯🇴", "Turkey": "🇹🇷", "Egypt": "🇪🇬",
    "Ukraine": "🇺🇦", "Russia": "🇷🇺", "China": "🇨🇳", "India": "🇮🇳",
    "Pakistan": "🇵🇰", "Afghanistan": "🇦🇫", "North Korea": "🇰🇵",
    "Sudan": "🇸🇩", "Ethiopia": "🇪🇹", "Somalia": "🇸🇴", "Nigeria": "🇳🇬",
    "Libya": "🇱🇾", "Mali": "🇲🇱", "Colombia": "🇨🇴", "Venezuela": "🇻🇪",
    "Mexico": "🇲🇽", "United States": "🇺🇸", "Myanmar": "🇲🇲",
}

DARK_THEME_CSS = """
<style>
    /* Main background */
    .stApp { background-color: #0a0a0f; color: #e0e0e0; }
    .main .block-container { padding-top: 1rem; max-width: 1400px; }

    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #0d0d1a; border-right: 1px solid #1a1a3e; }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stMultiselect label,
    [data-testid="stSidebar"] .stSlider label { color: #ff6b00 !important; font-weight: 600; }

    /* Headers */
    h1 { color: #ff6b00 !important; font-family: 'Courier New', monospace; font-size: 1.8rem !important; }
    h2 { color: #ffa500 !important; font-family: 'Courier New', monospace; }
    h3 { color: #ffcc00 !important; }

    /* Metric cards */
    [data-testid="stMetric"] { background: #0f0f2a; border: 1px solid #1a1a4e; border-radius: 8px; padding: 12px; }
    [data-testid="stMetricLabel"] { color: #aaa !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 1px; }
    [data-testid="stMetricValue"] { color: #ff6b00 !important; font-family: 'Courier New', monospace; font-size: 1.8rem !important; }
    [data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

    /* Buttons */
    .stButton > button { background: #1a1a4e; color: #ff6b00; border: 1px solid #ff6b00; border-radius: 4px; font-family: 'Courier New', monospace; }
    .stButton > button:hover { background: #ff6b00; color: #000; }

    /* Event cards */
    .event-card {
        background: #0f0f2a;
        border: 1px solid #1a1a4e;
        border-left: 4px solid #ff6b00;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 8px;
        font-family: 'Courier New', monospace;
    }
    .event-card-critical { border-left-color: #DC143C !important; }
    .event-card-high { border-left-color: #FF6400 !important; }
    .event-card-medium { border-left-color: #FFC800 !important; }
    .event-card-low { border-left-color: #1E90FF !important; }

    /* Severity badges */
    .badge { padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; font-weight: 700; font-family: 'Courier New', monospace; }
    .badge-CRITICAL { background: #DC143C; color: white; }
    .badge-HIGH { background: #FF6400; color: white; }
    .badge-MEDIUM { background: #FFC800; color: black; }
    .badge-LOW { background: #1E90FF; color: white; }

    /* Ticker */
    .ticker { background: #0d0d1a; border-top: 1px solid #ff6b00; padding: 6px 0; font-family: 'Courier New', monospace; font-size: 0.8rem; color: #ff6b00; white-space: nowrap; overflow: hidden; }

    /* Tables */
    .dataframe { background: #0f0f2a !important; color: #e0e0e0 !important; }

    /* Input boxes */
    .stTextInput > div > div > input { background: #0f0f2a; color: #e0e0e0; border: 1px solid #1a1a4e; }
    .stSelectbox > div > div { background: #0f0f2a; color: #e0e0e0; }

    /* Dividers */
    hr { border-color: #1a1a4e; }

    /* Tab styling */
    .stTabs [data-baseweb="tab"] { background: #0f0f2a; color: #888; border-bottom: 2px solid transparent; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #ff6b00; border-bottom-color: #ff6b00; }
</style>
"""

# ---------------------------------------------------------------------------
# API client helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def fetch_events(
    region: str = "ALL",
    severity: Optional[str] = None,
    event_type: Optional[str] = None,
    hours: int = 24,
    limit: int = 200,
) -> List[dict]:
    params = {"region": region, "hours": hours, "limit": limit}
    if severity:
        params["severity"] = severity
    if event_type:
        params["event_type"] = event_type
    try:
        r = httpx.get(f"{API_BASE}/api/events", params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        st.warning(f"API error fetching events: {exc}")
        return []


@st.cache_data(ttl=60)
def fetch_threat_index() -> Optional[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/threat-index", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


@st.cache_data(ttl=60)
def fetch_summary() -> List[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/summary", timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=60)
def fetch_hotspots(hours: int = 24) -> List[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/hotspots", params={"hours": hours, "top_n": 10}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=120)
def fetch_timeline(country: str, days: int = 30) -> List[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/timeline", params={"country": country, "days": days}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=30)
def fetch_search(q: str, limit: int = 50) -> List[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/search", params={"q": q, "limit": limit}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


@st.cache_data(ttl=120)
def fetch_stats() -> Optional[dict]:
    try:
        r = httpx.get(f"{API_BASE}/api/stats", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def time_ago(dt_str: str) -> str:
    """Human-readable time since published_at."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        dt = dt.replace(tzinfo=None)
        diff = datetime.utcnow() - dt
        seconds = diff.total_seconds()
        if seconds < 60:
            return f"{int(seconds)}s ago"
        if seconds < 3600:
            return f"{int(seconds/60)}m ago"
        if seconds < 86400:
            return f"{int(seconds/3600)}h ago"
        return f"{int(seconds/86400)}d ago"
    except Exception:
        return "unknown"


def severity_badge(severity: str) -> str:
    return f'<span class="badge badge-{severity}">{severity}</span>'


def events_to_df(events: List[dict]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()
    df = pd.DataFrame(events)
    for col in ["lat", "lon", "escalation_score", "sentiment_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Page 1 — Global Threat Map
# ---------------------------------------------------------------------------

def render_threat_map(region_filter: str, severity_filter: Optional[str], hours: int):
    st.markdown("## GLOBAL THREAT MAP")

    # Threat index gauge
    threat = fetch_threat_index()
    col1, col2, col3, col4, col5 = st.columns(5)

    if threat:
        score = threat.get("score", 0)
        level = threat.get("level", "UNKNOWN")
        trend = threat.get("trend_24h", 0)
        hotspots = threat.get("top_hotspots", [])
        counts = threat.get("event_counts", {})

        color = "#1E90FF" if score < 30 else "#FFC800" if score < 60 else "#FF6400" if score < 80 else "#DC143C"

        with col1:
            st.metric("THREAT INDEX", f"{score:.0f}/100", delta=f"{trend:+.1f} vs 24h ago")
        with col2:
            st.metric("THREAT LEVEL", level)
        with col3:
            st.metric("CRITICAL (24h)", counts.get("CRITICAL", 0))
        with col4:
            st.metric("HIGH (24h)", counts.get("HIGH", 0))
        with col5:
            st.metric("TOTAL (24h)", counts.get("total", 0))

        # Gauge chart
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=score,
            delta={"reference": score - trend, "valueformat": ".1f"},
            title={"text": "GLOBAL THREAT INDEX", "font": {"color": "#ff6b00", "size": 14}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#888", "tickfont": {"color": "#888"}},
                "bar": {"color": color},
                "bgcolor": "#0f0f2a",
                "bordercolor": "#1a1a4e",
                "steps": [
                    {"range": [0, 30], "color": "#0a1a2a"},
                    {"range": [30, 60], "color": "#1a1a0a"},
                    {"range": [60, 80], "color": "#1a0a0a"},
                    {"range": [80, 100], "color": "#2a0a0a"},
                ],
                "threshold": {"line": {"color": "#DC143C", "width": 3}, "thickness": 0.75, "value": 80},
            },
            number={"font": {"color": color, "size": 36}},
        ))
        fig_gauge.update_layout(
            paper_bgcolor="#0a0a0f", font_color="#e0e0e0", height=250, margin=dict(t=40, b=10, l=30, r=30)
        )
        st.plotly_chart(fig_gauge, use_container_width=True)

        if hotspots:
            st.markdown(f"**TOP HOTSPOTS:** {' | '.join([f'{COUNTRY_FLAGS.get(h, '')} {h}' for h in hotspots[:5]])}")

    # Map
    events = fetch_events(region=region_filter, severity=severity_filter, hours=hours, limit=500)
    df = events_to_df(events)

    if not df.empty and "lat" in df.columns:
        df_map = df.dropna(subset=["lat", "lon"])
        if not df_map.empty:
            df_map = df_map.copy()
            df_map["radius"] = df_map["severity"].map(SEVERITY_RADIUS).fillna(40000)
            df_map["color"] = df_map["severity"].map(SEVERITY_COLORS).apply(
                lambda x: x if isinstance(x, list) else [100, 100, 100, 150]
            )
            df_map["tooltip"] = df_map.apply(
                lambda r: f"{r.get('title','')[:80]} | {r.get('country','')} | {r.get('severity','')}",
                axis=1,
            )

            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_map[["lat", "lon", "radius", "color", "tooltip"]].to_dict("records"),
                get_position=["lon", "lat"],
                get_radius="radius",
                get_fill_color="color",
                pickable=True,
                opacity=0.85,
                stroked=True,
                get_line_color=[255, 100, 0, 180],
                line_width_min_pixels=1,
            )

            # Arc layer: connect events in same region
            arc_data = []
            if len(df_map) > 1:
                region_groups = df_map.groupby("region")
                for region_name, group in region_groups:
                    if len(group) >= 2:
                        rows = group.head(5).to_dict("records")
                        for i in range(len(rows) - 1):
                            arc_data.append({
                                "source_lat": rows[i]["lat"],
                                "source_lon": rows[i]["lon"],
                                "target_lat": rows[i + 1]["lat"],
                                "target_lon": rows[i + 1]["lon"],
                            })

            layers = [scatter_layer]
            if arc_data:
                arc_layer = pdk.Layer(
                    "ArcLayer",
                    data=arc_data,
                    get_source_position=["source_lon", "source_lat"],
                    get_target_position=["target_lon", "target_lat"],
                    get_source_color=[255, 100, 0, 120],
                    get_target_color=[220, 20, 60, 120],
                    auto_highlight=True,
                    width_min_pixels=1,
                    pickable=False,
                )
                layers.append(arc_layer)

            view_state = pdk.ViewState(
                latitude=25.0, longitude=45.0, zoom=2.5, pitch=35, bearing=0
            )

            deck = pdk.Deck(
                layers=layers,
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/dark-v11",
                tooltip={"text": "{tooltip}"},
            )

            st.pydeck_chart(deck, use_container_width=True)

            # Ticker
            if not df.empty:
                ticker_items = []
                for _, row in df.head(15).iterrows():
                    flag = COUNTRY_FLAGS.get(row.get("country", ""), "")
                    ticker_items.append(
                        f"{flag} [{row.get('severity','')}] {row.get('title','')[:60]} ({row.get('country','')})"
                    )
                ticker_text = "   ///   ".join(ticker_items)
                st.markdown(
                    f'<div class="ticker">&#x25B6; LIVE: {ticker_text}</div>',
                    unsafe_allow_html=True,
                )
    else:
        st.info("No events with coordinates available. Data will appear after ingestion cycle completes.")


# ---------------------------------------------------------------------------
# Page 2 — Middle East / Iran Focus
# ---------------------------------------------------------------------------

def render_middle_east():
    st.markdown("## MIDDLE EAST / IRAN INTELLIGENCE")

    me_countries = ["Iran", "Israel", "Lebanon", "Syria", "Iraq", "Yemen", "Gaza", "Palestine"]
    events_all = fetch_events(region="MIDDLE_EAST", hours=168, limit=500)  # 7 days
    df_all = events_to_df(events_all)

    # Country cards
    st.markdown("### 24H SITUATION REPORT")
    cols = st.columns(len(me_countries))

    for i, country in enumerate(me_countries):
        with cols[i]:
            if not df_all.empty and "country" in df_all.columns:
                cutoff_24h = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=24)
                if "published_at" in df_all.columns:
                    df_country_24h = df_all[
                        (df_all["country"] == country) &
                        (df_all["published_at"].dt.tz_localize(None) >= cutoff_24h if df_all["published_at"].dt.tz is not None
                         else df_all["published_at"] >= cutoff_24h)
                    ]
                else:
                    df_country_24h = df_all[df_all["country"] == country]

                count = len(df_country_24h)
                flag = COUNTRY_FLAGS.get(country, "")

                # Trend: compare to previous 24h
                cutoff_48h = cutoff_24h - pd.Timedelta(hours=24)
                if "published_at" in df_all.columns:
                    try:
                        published_no_tz = df_all["published_at"].dt.tz_localize(None) if df_all["published_at"].dt.tz is not None else df_all["published_at"]
                        prev_count = len(df_all[
                            (df_all["country"] == country) &
                            (published_no_tz >= cutoff_48h) &
                            (published_no_tz < cutoff_24h)
                        ])
                    except Exception:
                        prev_count = 0
                else:
                    prev_count = 0

                delta = count - prev_count
                trend_arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
                trend_color = "red" if delta > 0 else ("green" if delta < 0 else "gray")

                st.markdown(
                    f"""
                    <div style="background:#0f0f2a;border:1px solid #1a1a4e;border-radius:8px;padding:10px;text-align:center;">
                        <div style="font-size:1.8rem">{flag}</div>
                        <div style="color:#aaa;font-size:0.7rem;text-transform:uppercase">{country}</div>
                        <div style="color:#ff6b00;font-size:1.5rem;font-weight:bold">{count}</div>
                        <div style="color:{trend_color};font-size:1rem">{trend_arrow} {abs(delta)}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(f"**{COUNTRY_FLAGS.get(country, '')} {country}**\n\nNo data")

    st.markdown("---")

    col_left, col_right = st.columns([3, 2])

    with col_left:
        # Timeline chart
        st.markdown("### 30-DAY EVENT TIMELINE")
        if not df_all.empty and "published_at" in df_all.columns:
            df_timeline = df_all.copy()
            try:
                df_timeline["date"] = pd.to_datetime(df_timeline["published_at"]).dt.date
                daily = df_timeline.groupby(["date", "event_type"]).size().reset_index(name="count")
                if not daily.empty:
                    fig_timeline = px.bar(
                        daily, x="date", y="count", color="event_type",
                        title="Events Per Day by Type — Middle East",
                        color_discrete_map={
                            "AIRSTRIKE": "#DC143C", "MISSILE_ATTACK": "#FF6400",
                            "GROUND_OPERATION": "#FFC800", "NAVAL": "#1E90FF",
                            "CYBER": "#9B59B6", "PROTEST": "#2ECC71",
                            "CEASEFIRE": "#27AE60", "DIPLOMATIC": "#3498DB",
                            "SANCTIONS": "#E67E22", "UNKNOWN": "#555",
                        },
                    )
                    fig_timeline.update_layout(
                        paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                        font_color="#e0e0e0", legend_bgcolor="#0f0f2a",
                        xaxis=dict(gridcolor="#1a1a4e"),
                        yaxis=dict(gridcolor="#1a1a4e"),
                        height=320,
                    )
                    st.plotly_chart(fig_timeline, use_container_width=True)
            except Exception as exc:
                st.warning(f"Timeline chart error: {exc}")

    with col_right:
        # Key actors network
        st.markdown("### KEY ACTORS")
        if not df_all.empty and "entities" in df_all.columns:
            from collections import Counter
            all_entities = []
            for ents in df_all["entities"].dropna():
                if isinstance(ents, list):
                    all_entities.extend(ents)
                elif isinstance(ents, str):
                    try:
                        parsed = json.loads(ents)
                        all_entities.extend(parsed)
                    except Exception:
                        pass

            if all_entities:
                entity_counts = Counter(all_entities).most_common(15)
                ent_df = pd.DataFrame(entity_counts, columns=["entity", "mentions"])
                fig_entities = px.bar(
                    ent_df, x="mentions", y="entity", orientation="h",
                    title="Most Mentioned Entities",
                    color="mentions",
                    color_continuous_scale=["#1E90FF", "#FF6400", "#DC143C"],
                )
                fig_entities.update_layout(
                    paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                    font_color="#e0e0e0", height=320,
                    yaxis={"categoryorder": "total ascending"},
                    showlegend=False,
                )
                st.plotly_chart(fig_entities, use_container_width=True)

    st.markdown("---")

    # Iran Nuclear Tracker
    st.markdown("### IRAN NUCLEAR PROGRAM TRACKER")
    iran_events = fetch_search("Iran nuclear", limit=20)
    if iran_events:
        for event in iran_events[:8]:
            sev = event.get("severity", "LOW")
            flag = COUNTRY_FLAGS.get(event.get("country", ""), "")
            icon = EVENT_TYPE_ICONS.get(event.get("event_type", ""), "")
            st.markdown(
                f"""
                <div class="event-card event-card-{sev.lower()}">
                    <span class="badge badge-{sev}">{sev}</span>
                    {flag} {icon} <strong>{event.get('title','')}</strong><br>
                    <small style="color:#888">{event.get('source_name','')} · {time_ago(event.get('published_at',''))}</small>
                </div>
                """,
                unsafe_allow_html=True,
            )
    else:
        st.info("No Iran nuclear events found. Will populate after ingestion.")

    # Latest ME events feed
    st.markdown("### LATEST EVENTS")
    me_events = fetch_events(region="MIDDLE_EAST", hours=24, limit=20)
    for event in me_events[:10]:
        sev = event.get("severity", "LOW")
        flag = COUNTRY_FLAGS.get(event.get("country", ""), "")
        icon = EVENT_TYPE_ICONS.get(event.get("event_type", ""), "")
        esc = event.get("escalation_score", 0)
        st.markdown(
            f"""
            <div class="event-card event-card-{sev.lower()}">
                {severity_badge(sev)} {flag} {icon} <strong>{event.get('title','')}</strong><br>
                <small style="color:#888">{event.get('source_name','')} · {event.get('country','')} · {time_ago(event.get('published_at',''))}</small>
                <br><small style="color:#666">Escalation: <span style="color:#ff6b00">{esc:.2f}</span></small>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Page 3 — Live Event Feed
# ---------------------------------------------------------------------------

def render_live_feed():
    st.markdown("## LIVE EVENT FEED")

    # Filter bar
    col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
    with col1:
        country_search = st.text_input("Filter by country", placeholder="e.g. Iran, Ukraine...")
    with col2:
        type_filter = st.multiselect(
            "Event type",
            options=list(EVENT_TYPE_ICONS.keys()),
            default=[],
        )
    with col3:
        sev_filter = st.multiselect(
            "Severity",
            options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            default=[],
        )
    with col4:
        time_range = st.selectbox("Time", ["6h", "24h", "48h", "7d"], index=1)

    hours_map = {"6h": 6, "24h": 24, "48h": 48, "7d": 168}
    hours = hours_map.get(time_range, 24)

    events = fetch_events(hours=hours, limit=300)

    # Apply filters
    filtered = events
    if country_search:
        filtered = [e for e in filtered if country_search.lower() in e.get("country", "").lower()]
    if type_filter:
        filtered = [e for e in filtered if e.get("event_type") in type_filter]
    if sev_filter:
        filtered = [e for e in filtered if e.get("severity") in sev_filter]

    st.markdown(f"**{len(filtered)} events** matching filters")
    st.markdown("---")

    if not filtered:
        st.info("No events match the current filters.")
        return

    for event in filtered[:50]:
        sev = event.get("severity", "LOW")
        flag = COUNTRY_FLAGS.get(event.get("country", ""), "")
        icon = EVENT_TYPE_ICONS.get(event.get("event_type", ""), "")
        esc = event.get("escalation_score", 0)
        entities = event.get("entities", [])
        entities_str = ", ".join(entities[:5]) if entities else "None extracted"

        with st.expander(
            f"{severity_badge(sev)} {flag} {icon} {event.get('title','')[:90]}",
            expanded=False,
        ):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"**Description:** {event.get('description','No description available.')[:500]}")
                st.markdown(f"**Entities:** {entities_str}")
                st.markdown(f"**Keywords:** {', '.join(event.get('keywords', [])[:8])}")
                if event.get("source_url"):
                    st.markdown(f"**Source:** [{event.get('source_name','')}]({event.get('source_url','')})")
            with col_b:
                st.metric("Escalation", f"{esc:.2f}")
                st.metric("Sentiment", f"{event.get('sentiment_score',0):.2f}")
                st.metric("Event Type", event.get("event_type", ""))
                st.metric("Country", f"{flag} {event.get('country','')}")
                st.metric("Published", time_ago(event.get("published_at", "")))

            # Escalation bar
            esc_pct = int(esc * 100)
            esc_color = "#DC143C" if esc > 0.7 else "#FF6400" if esc > 0.4 else "#FFC800"
            st.markdown(
                f"""
                <div style="background:#1a1a4e;border-radius:4px;height:8px;margin:8px 0">
                    <div style="background:{esc_color};width:{esc_pct}%;height:100%;border-radius:4px"></div>
                </div>
                <small style="color:#888">Escalation probability: {esc_pct}%</small>
                """,
                unsafe_allow_html=True,
            )


# ---------------------------------------------------------------------------
# Page 4 — Analytics
# ---------------------------------------------------------------------------

def render_analytics():
    st.markdown("## CONFLICT ANALYTICS")

    events_7d = fetch_events(hours=168, limit=1000)
    df = events_to_df(events_7d)

    if df.empty:
        st.info("No data available yet. Analytics will populate after ingestion cycles complete.")
        return

    # Top row metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("7-DAY EVENTS", len(df))
    with col2:
        critical_count = len(df[df["severity"] == "CRITICAL"]) if "severity" in df.columns else 0
        st.metric("CRITICAL", critical_count)
    with col3:
        if "escalation_score" in df.columns:
            avg_esc = df["escalation_score"].mean()
            st.metric("AVG ESCALATION", f"{avg_esc:.2f}")
    with col4:
        countries = df["country"].nunique() if "country" in df.columns else 0
        st.metric("COUNTRIES AFFECTED", countries)

    st.markdown("---")

    col_l, col_r = st.columns(2)

    with col_l:
        # Events by country
        st.markdown("### EVENTS BY COUNTRY (TOP 20)")
        if "country" in df.columns:
            country_counts = df["country"].value_counts().head(20).reset_index()
            country_counts.columns = ["country", "count"]
            fig_country = px.bar(
                country_counts, x="count", y="country", orientation="h",
                color="count",
                color_continuous_scale=["#1E90FF", "#FFC800", "#FF6400", "#DC143C"],
            )
            fig_country.update_layout(
                paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                font_color="#e0e0e0", height=500,
                yaxis={"categoryorder": "total ascending"},
                showlegend=False,
            )
            st.plotly_chart(fig_country, use_container_width=True)

        # Escalation score histogram
        st.markdown("### ESCALATION SCORE DISTRIBUTION")
        if "escalation_score" in df.columns:
            fig_hist = px.histogram(
                df, x="escalation_score", nbins=20,
                color_discrete_sequence=["#FF6400"],
                title="Distribution of Escalation Scores",
            )
            fig_hist.update_layout(
                paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                font_color="#e0e0e0",
                xaxis=dict(gridcolor="#1a1a4e"),
                yaxis=dict(gridcolor="#1a1a4e"),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

    with col_r:
        # Event type pie
        st.markdown("### EVENT TYPE DISTRIBUTION")
        if "event_type" in df.columns:
            type_counts = df["event_type"].value_counts().reset_index()
            type_counts.columns = ["type", "count"]
            fig_pie = px.pie(
                type_counts, values="count", names="type",
                color_discrete_sequence=px.colors.sequential.RdBu,
                hole=0.4,
            )
            fig_pie.update_layout(
                paper_bgcolor="#0a0a0f", font_color="#e0e0e0",
                legend_bgcolor="#0f0f2a", height=350,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # Severity distribution over time
        st.markdown("### SEVERITY OVER TIME")
        if "published_at" in df.columns and "severity" in df.columns:
            try:
                df_sev = df.copy()
                df_sev["date"] = pd.to_datetime(df_sev["published_at"]).dt.date
                sev_daily = df_sev.groupby(["date", "severity"]).size().reset_index(name="count")
                fig_sev = px.area(
                    sev_daily, x="date", y="count", color="severity",
                    color_discrete_map={
                        "CRITICAL": "#DC143C", "HIGH": "#FF6400",
                        "MEDIUM": "#FFC800", "LOW": "#1E90FF",
                    },
                )
                fig_sev.update_layout(
                    paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                    font_color="#e0e0e0", height=300,
                    xaxis=dict(gridcolor="#1a1a4e"),
                    yaxis=dict(gridcolor="#1a1a4e"),
                )
                st.plotly_chart(fig_sev, use_container_width=True)
            except Exception as exc:
                st.warning(f"Severity chart error: {exc}")

    st.markdown("---")

    # 30-day global trend
    st.markdown("### 30-DAY GLOBAL CONFLICT TREND")
    events_30d = fetch_events(hours=720, limit=2000)
    df_30 = events_to_df(events_30d)
    if not df_30.empty and "published_at" in df_30.columns:
        try:
            df_30["date"] = pd.to_datetime(df_30["published_at"]).dt.date
            daily_total = df_30.groupby("date").size().reset_index(name="events")
            daily_total["rolling_7d"] = daily_total["events"].rolling(7, min_periods=1).mean()

            fig_trend = go.Figure()
            fig_trend.add_trace(go.Bar(
                x=daily_total["date"], y=daily_total["events"],
                name="Daily Events", marker_color="#1a1a4e",
            ))
            fig_trend.add_trace(go.Scatter(
                x=daily_total["date"], y=daily_total["rolling_7d"],
                name="7-Day Rolling Average", line=dict(color="#FF6400", width=3),
            ))
            fig_trend.update_layout(
                paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
                font_color="#e0e0e0", height=300,
                xaxis=dict(gridcolor="#1a1a4e"),
                yaxis=dict(gridcolor="#1a1a4e"),
                legend_bgcolor="#0f0f2a",
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            # Trend assessment
            if len(daily_total) >= 14:
                first_half = daily_total.head(len(daily_total) // 2)["events"].mean()
                second_half = daily_total.tail(len(daily_total) // 2)["events"].mean()
                change_pct = ((second_half - first_half) / first_half * 100) if first_half > 0 else 0
                direction = "INCREASING" if change_pct > 10 else ("DECREASING" if change_pct < -10 else "STABLE")
                color = "#DC143C" if direction == "INCREASING" else ("#2ECC71" if direction == "DECREASING" else "#FFC800")
                st.markdown(
                    f"**Global conflict trend: <span style='color:{color}'>{direction}</span> "
                    f"({change_pct:+.1f}% vs prior period)**",
                    unsafe_allow_html=True,
                )
        except Exception as exc:
            st.warning(f"Trend chart error: {exc}")

    # Source breakdown
    st.markdown("### SOURCE RELIABILITY BREAKDOWN")
    if not df_30.empty and "source_name" in df_30.columns:
        source_counts = df_30["source_name"].value_counts().reset_index()
        source_counts.columns = ["source", "count"]
        fig_sources = px.bar(
            source_counts, x="count", y="source", orientation="h",
            color="count",
            color_continuous_scale=["#1E90FF", "#FF6400"],
            title="Events by Source",
        )
        fig_sources.update_layout(
            paper_bgcolor="#0a0a0f", plot_bgcolor="#0f0f2a",
            font_color="#e0e0e0",
            yaxis={"categoryorder": "total ascending"},
            showlegend=False,
        )
        st.plotly_chart(fig_sources, use_container_width=True)


# ---------------------------------------------------------------------------
# Page 5 — Search & Intelligence
# ---------------------------------------------------------------------------

def render_search():
    st.markdown("## SEARCH & INTELLIGENCE")

    # Preset buttons
    col1, col2, col3, col4 = st.columns(4)
    preset_query = ""
    with col1:
        if st.button("Iran Nuclear", use_container_width=True):
            preset_query = "Iran nuclear"
    with col2:
        if st.button("Russia Ukraine", use_container_width=True):
            preset_query = "Russia Ukraine war"
    with col3:
        if st.button("Middle East", use_container_width=True):
            preset_query = "Middle East conflict"
    with col4:
        if st.button("Gaza Israel", use_container_width=True):
            preset_query = "Gaza Israel airstrike"

    query = st.text_input(
        "Search events",
        value=preset_query,
        placeholder="Enter keywords: Iran, missile, airstrike, ceasefire...",
    )

    col_l, col_r = st.columns([1, 3])
    with col_l:
        result_limit = st.slider("Max results", 10, 200, 50)

    if not query:
        st.info("Enter a search query or click a preset above.")
        return

    with st.spinner(f"Searching for '{query}'..."):
        results = fetch_search(query, limit=result_limit)

    if not results:
        st.warning(f"No results found for '{query}'")
        return

    st.success(f"Found **{len(results)}** events matching '{query}'")

    # Export button
    df_results = events_to_df(results)
    if not df_results.empty:
        csv_data = df_results[
            [c for c in ["title", "country", "region", "event_type", "severity",
                          "escalation_score", "published_at", "source_name", "source_url"]
             if c in df_results.columns]
        ].to_csv(index=False)

        st.download_button(
            label="Export Results as CSV",
            data=csv_data,
            file_name=f"conflictwatch_{query.replace(' ', '_')}_{datetime.utcnow().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    st.markdown("---")

    for event in results:
        sev = event.get("severity", "LOW")
        flag = COUNTRY_FLAGS.get(event.get("country", ""), "")
        icon = EVENT_TYPE_ICONS.get(event.get("event_type", ""), "")
        esc = event.get("escalation_score", 0)
        entities = event.get("entities", [])

        with st.expander(
            f"{severity_badge(sev)} {flag} {icon} {event.get('title','')[:100]}",
            expanded=False,
        ):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                desc = event.get("description", "")
                st.markdown(f"{desc[:600] if desc else '_No description available._'}")
                if entities:
                    st.markdown(f"**Key Actors:** {', '.join(entities[:8])}")
                kw = event.get("keywords", [])
                if kw:
                    st.markdown(f"**Keywords:** {', '.join(kw[:10])}")
                src_url = event.get("source_url", "")
                if src_url:
                    st.markdown(f"[Read Full Article]({src_url})")
            with col_b:
                st.metric("Severity", sev)
                st.metric("Escalation", f"{esc:.2f}")
                st.metric("Country", f"{flag} {event.get('country','')}")
                st.metric("Source", event.get("source_name", "")[:20])
                st.metric("Time", time_ago(event.get("published_at", "")))


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

def render_sidebar() -> tuple:
    with st.sidebar:
        st.markdown(
            """
            <div style="text-align:center;padding:10px 0">
                <div style="color:#ff6b00;font-size:1.2rem;font-weight:bold;font-family:'Courier New'">
                    ⚡ CONFLICTWATCH
                </div>
                <div style="color:#888;font-size:0.7rem">Global Conflict Intelligence</div>
                <div style="color:#666;font-size:0.65rem">Built by Hamza Ahmad | ETS Montreal</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("---")

        page = st.radio(
            "NAVIGATION",
            options=[
                "GLOBAL THREAT MAP",
                "MIDDLE EAST / IRAN",
                "LIVE FEED",
                "ANALYTICS",
                "SEARCH & INTEL",
            ],
            index=0,
        )

        st.markdown("---")
        st.markdown("**MAP FILTERS**")

        region_filter = st.selectbox(
            "Region",
            ["ALL", "MIDDLE_EAST", "EUROPE", "ASIA", "AFRICA", "AMERICAS"],
            index=0,
        )

        severity_filter = st.selectbox(
            "Severity",
            ["All", "CRITICAL", "HIGH", "MEDIUM", "LOW"],
            index=0,
        )

        time_range = st.selectbox(
            "Time Range",
            ["Last 6h", "Last 24h", "Last 7d", "Last 30d"],
            index=1,
        )

        hours_map = {"Last 6h": 6, "Last 24h": 24, "Last 7d": 168, "Last 30d": 720}
        hours = hours_map.get(time_range, 24)

        st.markdown("---")

        # Live stats
        stats = fetch_stats()
        if stats:
            st.markdown("**INGESTION STATUS**")
            st.markdown(f"Total events: **{stats.get('total_events', 0):,}**")
            st.markdown(f"Last 24h: **{stats.get('events_24h', 0)}**")
            last_ingest = stats.get("last_ingestion_at")
            if last_ingest:
                st.markdown(f"Last update: **{time_ago(last_ingest)}**")

        st.markdown("---")
        st.markdown(
            '<div style="color:#444;font-size:0.65rem">Data: GDELT Project, Reuters,<br>BBC, Al Jazeera, AP, Iran Intl.<br>Updates every 15 minutes.</div>',
            unsafe_allow_html=True,
        )

    return (
        page,
        region_filter,
        None if severity_filter == "All" else severity_filter,
        hours,
    )


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(
        page_title="ConflictWatch — Global Conflict Intelligence",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Inject dark military theme CSS
    st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)

    # Auto-refresh counter
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = time.time()

    # Auto-refresh every 60 seconds
    elapsed = time.time() - st.session_state.last_refresh
    if elapsed > AUTO_REFRESH_SECONDS:
        st.cache_data.clear()
        st.session_state.last_refresh = time.time()
        st.rerun()

    page, region_filter, severity_filter, hours = render_sidebar()

    if page == "GLOBAL THREAT MAP":
        render_threat_map(region_filter, severity_filter, hours)
    elif page == "MIDDLE EAST / IRAN":
        render_middle_east()
    elif page == "LIVE FEED":
        render_live_feed()
    elif page == "ANALYTICS":
        render_analytics()
    elif page == "SEARCH & INTEL":
        render_search()

    # Footer
    st.markdown(
        """
        <div style="text-align:center;color:#333;font-size:0.65rem;padding:20px 0;border-top:1px solid #1a1a4e;margin-top:30px">
            ConflictWatch v1.0 | Built by Hamza Ahmad | ETS Montreal |
            Data sourced from GDELT Project and public news RSS feeds |
            For informational/research purposes only
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
