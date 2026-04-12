import os
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime

# ================= CONFIG =================
# DEMO_MODE: True when deployed to Streamlit Community Cloud (no backend).
# Set env var DEMO_MODE=false in your local .env or shell to use live API.
DEMO_MODE = os.environ.get("DEMO_MODE", "true").lower() == "true"

API_BASE  = "http://localhost:5000/api"
ACCOUNTS  = ['acct-001', 'acct-002', 'acct-003', 'acct-004', 'acct-005']

if DEMO_MODE:
    import demo_data

st.set_page_config(
    page_title="CloudWatch - Cost Intelligence",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ================= STYLING =================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@400;600;700;800&display=swap');

*, *::before, *::after { box-sizing: border-box; }

html, body, .stApp {
    background-color: #080C14;
    color: #C9D1E0;
    font-family: 'Syne', sans-serif;
}

.block-container { padding: 3.5rem 2rem 3rem 2rem; max-width: 100%; }

/* ---- Sidebar ---- */
section[data-testid="stSidebar"] {
    background: #0C1220;
    border-right: 1px solid #1A2236;
}
section[data-testid="stSidebar"] .block-container { padding: 2rem 1.2rem; }

.sidebar-logo {
    font-family: 'Syne', sans-serif;
    font-size: 15px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #4F8EF7;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 8px;
}
.sidebar-logo span { color: #C9D1E0; }

.sidebar-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #4A5568;
    margin-bottom: 6px;
    margin-top: 20px;
}

/* ---- Page Header ---- */
.page-header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    margin-bottom: 2rem;
    margin-top: 0.5rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid #1A2236;
}
.page-title {
    font-size: 26px;
    font-weight: 800;
    color: #E8EDF5;
    letter-spacing: -0.02em;
    line-height: 1;
}
.page-subtitle {
    font-size: 12px;
    color: #4A5568;
    margin-top: 6px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    font-weight: 600;
}
.page-ts {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: #2D3F5A;
    text-align: right;
}

/* ---- Metric Cards ---- */
.metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 1.5rem; }

.metric-card {
    background: #0C1220;
    border: 1px solid #1A2236;
    border-radius: 10px;
    padding: 18px 20px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
}
.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    border-radius: 10px 10px 0 0;
}
.metric-card.blue::before  { background: linear-gradient(90deg, #4F8EF7, transparent); }
.metric-card.green::before { background: linear-gradient(90deg, #22D3A0, transparent); }
.metric-card.amber::before { background: linear-gradient(90deg, #F59E0B, transparent); }
.metric-card.red::before   { background: linear-gradient(90deg, #EF4444, transparent); }

.mc-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: #4A5568;
    margin-bottom: 10px;
}
.mc-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 28px;
    font-weight: 600;
    color: #E8EDF5;
    line-height: 1;
    margin-bottom: 6px;
}
.mc-value.blue  { color: #4F8EF7; }
.mc-value.green { color: #22D3A0; }
.mc-value.amber { color: #F59E0B; }
.mc-value.red   { color: #EF4444; }

.mc-sub {
    font-size: 11px;
    color: #2D3F5A;
    font-family: 'JetBrains Mono', monospace;
}

/* ---- Section Headers ---- */
.section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 14px;
}
.section-title {
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #6B7A99;
}
.section-badge {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: #2D3F5A;
    background: #111827;
    border: 1px solid #1A2236;
    border-radius: 4px;
    padding: 2px 8px;
}

/* ---- Panel ---- */
.panel {
    background: #0C1220;
    border: 1px solid #1A2236;
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 14px;
}

/* ---- Insights ---- */
.insight-row {
    display: flex;
    gap: 10px;
    margin-bottom: 10px;
    flex-wrap: wrap;
}
.insight-chip {
    display: flex;
    align-items: center;
    gap: 8px;
    background: #111827;
    border: 1px solid #1A2236;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 12px;
    color: #8A9BBD;
    flex: 1;
    min-width: 200px;
}
.insight-chip .dot {
    width: 6px; height: 6px;
    border-radius: 50%;
    flex-shrink: 0;
}
.insight-chip .dot.blue  { background: #4F8EF7; }
.insight-chip .dot.green { background: #22D3A0; }
.insight-chip .dot.amber { background: #F59E0B; }
.insight-chip .dot.red   { background: #EF4444; }
.insight-chip strong { color: #C9D1E0; }

/* ---- Alert rows ---- */
.alert-item {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 10px 14px;
    border-radius: 6px;
    margin-bottom: 6px;
    font-size: 12px;
    line-height: 1.5;
    font-family: 'JetBrains Mono', monospace;
}
.alert-item.critical { background: rgba(239,68,68,0.07); border: 1px solid rgba(239,68,68,0.2); color: #FCA5A5; }
.alert-item.warning  { background: rgba(245,158,11,0.07); border: 1px solid rgba(245,158,11,0.2); color: #FCD34D; }
.alert-item.info     { background: rgba(79,142,247,0.07); border: 1px solid rgba(79,142,247,0.2); color: #93C5FD; }
.alert-severity { font-size: 10px; font-weight: 700; letter-spacing: 0.1em; padding: 2px 6px; border-radius: 3px; flex-shrink: 0; margin-top: 1px; }
.alert-item.critical .alert-severity { background: rgba(239,68,68,0.2); color: #EF4444; }
.alert-item.warning  .alert-severity { background: rgba(245,158,11,0.2); color: #F59E0B; }
.alert-item.info     .alert-severity { background: rgba(79,142,247,0.2); color: #4F8EF7; }

/* ---- Anomaly Stats ---- */
.anomaly-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.anomaly-cell {
    background: #111827;
    border: 1px solid #1A2236;
    border-radius: 7px;
    padding: 14px;
    text-align: center;
}
.anomaly-cell .ac-label {
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #4A5568;
    margin-bottom: 6px;
    font-weight: 600;
}
.anomaly-cell .ac-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 20px;
    font-weight: 600;
    color: #E8EDF5;
}
.anomaly-cell .ac-context { font-size: 10px; color: #2D3F5A; margin-top: 4px; }

/* ---- Recommendation table ---- */
.rec-row {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    border-radius: 6px;
    margin-bottom: 6px;
    background: #111827;
    border: 1px solid #1A2236;
    font-size: 12px;
}
.rec-badge {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.08em;
    padding: 3px 8px;
    border-radius: 4px;
    flex-shrink: 0;
}
.rec-badge.switch_to_reserved { background: rgba(79,142,247,0.15); color: #4F8EF7; border: 1px solid rgba(79,142,247,0.3); }
.rec-badge.rightsizing         { background: rgba(34,211,160,0.15); color: #22D3A0; border: 1px solid rgba(34,211,160,0.3); }
.rec-badge.delete_unused       { background: rgba(239,68,68,0.15);  color: #EF4444; border: 1px solid rgba(239,68,68,0.3); }
.rec-badge.default             { background: rgba(107,122,153,0.15); color: #6B7A99; border: 1px solid rgba(107,122,153,0.3); }

.rec-detail { flex: 1; color: #8A9BBD; }
.rec-saving { font-family: 'JetBrains Mono', monospace; color: #22D3A0; font-weight: 600; flex-shrink: 0; }

/* ---- Divider ---- */
.h-divider { border: none; border-top: 1px solid #1A2236; margin: 1.5rem 0; }

/* ---- Streamlit overrides ---- */
div[data-testid="stSelectbox"] label,
div[data-testid="stButton"] { margin-top: 0; }

.stButton > button {
    background: #111827;
    border: 1px solid #1A2236;
    color: #6B7A99;
    border-radius: 6px;
    font-family: 'Syne', sans-serif;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.08em;
    padding: 6px 16px;
    width: 100%;
    transition: all 0.2s;
}
.stButton > button:hover {
    border-color: #4F8EF7;
    color: #4F8EF7;
    background: rgba(79,142,247,0.05);
}

div[data-testid="stSelectbox"] > div > div {
    background: #111827;
    border: 1px solid #1A2236;
    border-radius: 6px;
    color: #C9D1E0;
    font-size: 13px;
}

.stPlotlyChart { border-radius: 8px; overflow: hidden; }

/* Hide streamlit chrome */
#MainMenu, footer { visibility: hidden; }
header { visibility: visible; }
[data-testid="collapsedControl"] { visibility: visible; }
</style>
""", unsafe_allow_html=True)

# ================= HELPERS =================
@st.cache_data(ttl=30)
def fetch_json(endpoint):
    resp = requests.get(f"{API_BASE}{endpoint}", timeout=5)
    resp.raise_for_status()
    return resp.json()

def safe_fetch(endpoint, default):
    try:
        return fetch_json(endpoint)
    except Exception:
        return default

def fmt_money(x):
    try:
        v = float(x)
        if v >= 1000:
            return f"${v:,.0f}"
        return f"${v:,.2f}"
    except Exception:
        return "$0.00"

def fmt_pct_change(current, previous):
    if not previous or previous == 0:
        return None
    delta = ((current - previous) / previous) * 100
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta:.1f}% vs prev period"

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="JetBrains Mono, monospace", size=11, color="#4A5568"),
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        font=dict(size=10, color="#6B7A99"),
        orientation="v",
    ),
    xaxis=dict(
        gridcolor="#1A2236",
        linecolor="#1A2236",
        tickcolor="#1A2236",
        showgrid=True,
        zeroline=False,
    ),
    yaxis=dict(
        gridcolor="#1A2236",
        linecolor="#1A2236",
        tickcolor="#1A2236",
        showgrid=True,
        zeroline=False,
        tickprefix="$",
    ),
)

ACCENT_COLORS = ["#4F8EF7", "#22D3A0", "#F59E0B", "#EF4444", "#A78BFA", "#FB923C"]

# ================= DATA FETCH =================
_acct = st.session_state.get('account', ACCOUNTS[0])

if DEMO_MODE:
    dashboard   = demo_data.get_dashboard(_acct)
    trend       = demo_data.get_trend(_acct)
    recs        = demo_data.get_recommendations(_acct)
    alerts_resp = demo_data.get_alerts(_acct)
    anomaly     = demo_data.get_anomaly_stats(_acct)
    costs_resp  = demo_data.get_costs(_acct)
    region_data = demo_data.get_region_data()
else:
    dashboard   = safe_fetch(f"/dashboard/{_acct}",        {"data": {}})
    trend       = safe_fetch(f"/trend/{_acct}",            {"data": []})
    recs        = safe_fetch(f"/recommendations/{_acct}",  {"data": [], "by_type": {}, "total_monthly_savings": 0})
    alerts_resp = safe_fetch(f"/alerts/{_acct}",           {"data": []})
    anomaly     = safe_fetch(f"/anomaly-stats/{_acct}",    {"data": {}})
    costs_resp  = safe_fetch(f"/costs/{_acct}",            {"data": []})
    region_data = safe_fetch("/usage/by-region/ap-south-1",{"service_cost_summary": {}})

data        = dashboard.get("data", {})
total_spend = float(data.get("total_spend", 0) or 0)
savings_pot = float(recs.get("total_monthly_savings", 0) or 0)
alert_count = int(data.get("alert_count", 0) or 0)
resources   = int(data.get("active_resources", 0) or 0)
top_service = data.get("top_service", "N/A")
anomaly_stats = anomaly.get("data", {})

# ================= SIDEBAR =================
with st.sidebar:
    st.markdown('<div class="sidebar-logo">Cloud<span>Watch</span></div>', unsafe_allow_html=True)

    if DEMO_MODE:
        st.markdown("""
        <div style="background:rgba(79,142,247,0.08);border:1px solid rgba(79,142,247,0.25);
                    border-radius:7px;padding:10px 12px;margin-bottom:16px;">
            <div style="font-size:10px;font-weight:700;letter-spacing:0.12em;
                        color:#4F8EF7;text-transform:uppercase;margin-bottom:4px">Demo Mode</div>
            <div style="font-size:11px;color:#6B7A99;line-height:1.5">
                Showing realistic static data.<br>
                <a href="https://github.com/kvkushal/CloudWatch"
                   target="_blank"
                   style="color:#4F8EF7;text-decoration:none">View source on GitHub</a>
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('<div class="sidebar-label">Account</div>', unsafe_allow_html=True)
    selected_account = st.selectbox("Account", ACCOUNTS, label_visibility="collapsed", key="account")
    st.markdown('<div class="sidebar-label">Actions</div>', unsafe_allow_html=True)
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.markdown('<div class="sidebar-label">Account Summary</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#4A5568;line-height:2">
        Resources &nbsp;&nbsp;&nbsp;<span style="color:#C9D1E0">{resources}</span><br>
        Alerts &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span style="color:{'#EF4444' if alert_count > 0 else '#22D3A0'}">{alert_count}</span><br>
        Top service &nbsp;<span style="color:#C9D1E0">{top_service}</span>
    </div>
    """, unsafe_allow_html=True)

# ================= HEADER =================
now_str = datetime.utcnow().strftime("UTC %Y-%m-%d %H:%M")
st.markdown(f"""
<div class="page-header">
    <div>
        <div class="page-title">Cost Intelligence</div>
        <div class="page-subtitle">{selected_account} &nbsp;/&nbsp; ap-south-1</div>
    </div>
    <div class="page-ts">Last synced: {now_str}</div>
</div>
""", unsafe_allow_html=True)

# ================= METRIC CARDS =================
savings_ratio = (savings_pot / total_spend * 100) if total_spend > 0 else 0

st.markdown(f"""
<div class="metric-grid">
    <div class="metric-card blue">
        <div class="mc-label">Current Spend</div>
        <div class="mc-value blue">{fmt_money(total_spend)}</div>
        <div class="mc-sub">Today &mdash; rolling 24h</div>
    </div>
    <div class="metric-card green">
        <div class="mc-label">Savings Opportunity</div>
        <div class="mc-value green">{fmt_money(savings_pot)}</div>
        <div class="mc-sub">{savings_ratio:.1f}% of current spend</div>
    </div>
    <div class="metric-card {'red' if alert_count > 0 else 'green'}">
        <div class="mc-label">Active Alerts</div>
        <div class="mc-value {'red' if alert_count > 0 else 'green'}">{alert_count}</div>
        <div class="mc-sub">{'Action required' if alert_count > 0 else 'All clear'}</div>
    </div>
    <div class="metric-card amber">
        <div class="mc-label">Active Resources</div>
        <div class="mc-value amber">{resources}</div>
        <div class="mc-sub">Across all services</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ================= KEY INSIGHTS =================
insights = []
if savings_pot > total_spend:
    insights.append(("red", f"Savings opportunity ({fmt_money(savings_pot)}) exceeds today's spend ({fmt_money(total_spend)}). Consider immediate rightsizing."))
elif savings_ratio > 50:
    insights.append(("amber", f"Over half your current spend is recoverable. {savings_ratio:.1f}% savings potential identified."))
elif savings_ratio > 20:
    insights.append(("blue", f"{savings_ratio:.1f}% savings potential detected. Review recommendations below."))

if alert_count > 0:
    insights.append(("red", f"{alert_count} active alert{'s' if alert_count > 1 else ''} require attention. Check anomaly section for z-score details."))

if top_service:
    insights.append(("amber", f"{top_service} is the highest-cost service this period. Confirm it aligns with expected workload."))

anom_count = anomaly_stats.get("count", 0)
anom_mean  = anomaly_stats.get("mean", 0)
anom_std   = anomaly_stats.get("std_dev", 0)
if anom_count:
    cv = (anom_std / anom_mean * 100) if anom_mean else 0
    if cv > 30:
        insights.append(("red", f"High cost volatility: coefficient of variation is {cv:.1f}%. Daily spend is unpredictable."))
    elif cv > 15:
        insights.append(("amber", f"Moderate cost variance detected (CV: {cv:.1f}%). Monitor for unexpected spikes."))

if not insights:
    insights.append(("green", "No significant anomalies detected. Account is within expected cost parameters."))

chips_html = "".join([
    f'<div class="insight-chip"><div class="dot {color}"></div><div><strong></strong>{msg}</div></div>'
    for color, msg in insights
])
st.markdown(f"""
<div class="panel">
    <div class="section-header">
        <div class="section-title">Automated Insights</div>
        <div class="section-badge">{len(insights)} signals</div>
    </div>
    <div class="insight-row">{chips_html}</div>
</div>
""", unsafe_allow_html=True)

# ================= TREND + SERVICE SPLIT =================
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">Daily Cost Trend</div>
        <div class="section-badge">30d rolling</div>
    </div>
    """, unsafe_allow_html=True)

    df_trend = pd.DataFrame(trend.get("data", []))
    if not df_trend.empty and "date" in df_trend.columns and "total_cost" in df_trend.columns:
        df_trend["date"] = pd.to_datetime(df_trend["date"])
        df_trend = df_trend.sort_values("date")

        mean_cost = df_trend["total_cost"].mean()
        std_cost  = df_trend["total_cost"].std()
        anomaly_threshold = mean_cost + 2 * std_cost

        fig = go.Figure()

        # Confidence band
        fig.add_trace(go.Scatter(
            x=pd.concat([df_trend["date"], df_trend["date"][::-1]]),
            y=pd.concat([
                pd.Series([mean_cost + std_cost] * len(df_trend)),
                pd.Series([mean_cost - std_cost] * len(df_trend))
            ]),
            fill="toself",
            fillcolor="rgba(79,142,247,0.06)",
            line=dict(color="rgba(0,0,0,0)"),
            name="1σ band",
            hoverinfo="skip",
        ))

        # Mean line
        fig.add_trace(go.Scatter(
            x=df_trend["date"],
            y=[mean_cost] * len(df_trend),
            mode="lines",
            line=dict(color="#2D3F5A", width=1, dash="dash"),
            name=f"Avg {fmt_money(mean_cost)}",
        ))

        # Main cost line
        fig.add_trace(go.Scatter(
            x=df_trend["date"],
            y=df_trend["total_cost"],
            mode="lines+markers",
            line=dict(color="#4F8EF7", width=2),
            marker=dict(
                size=[10 if v > anomaly_threshold else 5 for v in df_trend["total_cost"]],
                color=["#EF4444" if v > anomaly_threshold else "#4F8EF7" for v in df_trend["total_cost"]],
                line=dict(color="#080C14", width=1.5),
            ),
            name="Daily cost",
            hovertemplate="<b>%{x|%b %d}</b><br>$%{y:,.2f}<extra></extra>",
        ))

        _exclude = {"legend", "xaxis", "yaxis"}
        trend_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in _exclude}
        fig.update_layout(
            **trend_layout,
            height=260,
            showlegend=True,
            legend=dict(orientation="h", y=-0.15, x=0),
            xaxis=dict(**CHART_LAYOUT["xaxis"], tickformat="%b %d"),
            yaxis=dict(**CHART_LAYOUT["yaxis"]),
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)
        st.markdown(
            f'<div style="font-size:10px;color:#2D3F5A;font-family:JetBrains Mono,monospace;">'
            f'Red markers indicate days where cost exceeded the 2σ anomaly threshold '
            f'({fmt_money(anomaly_threshold)}). Shaded band = ±1 standard deviation around mean.</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown('<div style="color:#4A5568;font-size:12px;padding:40px 0;text-align:center;">No trend data available</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">Service Cost Split</div>
        <div class="section-badge">Latest period</div>
    </div>
    """, unsafe_allow_html=True)

    service_breakdown = {}
    cost_data = costs_resp.get("data", [])
    if cost_data:
        latest_entry = cost_data[-1]
        service_breakdown = latest_entry.get("service_breakdown", {})

    if service_breakdown:
        total_sb = sum(service_breakdown.values())
        pie_df = pd.DataFrame({
            "service": list(service_breakdown.keys()),
            "cost": list(service_breakdown.values()),
        }).sort_values("cost", ascending=False)

        fig = go.Figure(go.Pie(
            labels=pie_df["service"],
            values=pie_df["cost"],
            hole=0.6,
            marker=dict(colors=ACCENT_COLORS, line=dict(color="#080C14", width=2)),
            textinfo="percent",
            textfont=dict(size=10, family="JetBrains Mono, monospace"),
            hovertemplate="<b>%{label}</b><br>$%{value:,.2f} &mdash; %{percent}<extra></extra>",
        ))

        center_label = pie_df.iloc[0]["service"] if not pie_df.empty else ""
        center_val   = fmt_money(pie_df.iloc[0]["cost"]) if not pie_df.empty else ""

        pie_layout = {k: v for k, v in CHART_LAYOUT.items() if k != "legend"}
        fig.update_layout(
            **pie_layout,
            height=240,
            showlegend=True,
            legend=dict(orientation="v", x=0.85, y=0.5, font=dict(size=10)),
            annotations=[dict(
                text=f'<b style="font-size:14px">{center_val}</b><br><span style="font-size:9px;color:#4A5568">{center_label}</span>',
                x=0.5, y=0.5,
                font=dict(size=11, color="#C9D1E0", family="JetBrains Mono, monospace"),
                showarrow=False
            )],
        )
        st.plotly_chart(fig, use_container_width=True)

        # Ranked list below the donut
        for _, row in pie_df.iterrows():
            pct = (row["cost"] / total_sb * 100) if total_sb else 0
            color_idx = list(pie_df["service"]).index(row["service"]) % len(ACCENT_COLORS)
            c = ACCENT_COLORS[color_idx]
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px;">
                <div style="width:8px;height:8px;border-radius:50%;background:{c};flex-shrink:0;"></div>
                <div style="font-size:11px;color:#8A9BBD;flex:1;">{row['service']}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:11px;color:#C9D1E0;">{fmt_money(row['cost'])}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:10px;color:#4A5568;width:36px;text-align:right">{pct:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#4A5568;font-size:12px;padding:40px 0;text-align:center;">No service data available</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ================= RECOMMENDATIONS =================
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    rec_items = recs.get("data", [])
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">Optimization Recommendations</div>
        <div class="section-badge">{len(rec_items)} items</div>
    </div>
    """, unsafe_allow_html=True)

    if rec_items:
        rec_df = pd.DataFrame(rec_items)

        # Parse details JSON if it's a string
        def parse_details(d):
            if isinstance(d, str):
                try:
                    return json.loads(d)
                except Exception:
                    return {}
            return d if isinstance(d, dict) else {}

        rec_df["_details"] = rec_df.get("details", pd.Series([{}] * len(rec_df))).apply(parse_details)
        rec_df = rec_df.sort_values("estimated_monthly_savings", ascending=False)

        for _, row in rec_df.head(10).iterrows():
            rec_type    = str(row.get("rec_type", row.get("type", "default"))).lower().replace(" ", "_")
            savings_val = row.get("estimated_monthly_savings", 0)
            details     = row.get("_details", {})

            # Build human-readable detail string
            detail_parts = []
            if "days_active" in details:
                detail_parts.append(f"Active {details['days_active']}d")
            if "savings_pct" in details:
                detail_parts.append(f"{details['savings_pct']}% savings rate")
            if "resource_id" in row and pd.notna(row.get("resource_id")):
                detail_parts.append(str(row.get("resource_id", "")))
            detail_str = " &bull; ".join(detail_parts) if detail_parts else "Review recommended"

            badge_class = rec_type if rec_type in ("switch_to_reserved", "rightsizing", "delete_unused") else "default"
            label_map = {
                "switch_to_reserved": "RESERVED",
                "rightsizing":        "RIGHTSIZE",
                "delete_unused":      "DELETE",
            }
            label = label_map.get(rec_type, rec_type.upper()[:10])

            st.markdown(f"""
            <div class="rec-row">
                <div class="rec-badge {badge_class}">{label}</div>
                <div class="rec-detail">{detail_str}</div>
                <div class="rec-saving">{fmt_money(savings_val)}/mo</div>
            </div>
            """, unsafe_allow_html=True)

        if len(rec_df) > 10:
            st.markdown(f'<div style="font-size:11px;color:#2D3F5A;padding:6px 14px">+{len(rec_df) - 10} more recommendations not shown</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#4A5568;font-size:12px;padding:20px 0;text-align:center;">No recommendations available</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">Recommendation Types</div>
    </div>
    """, unsafe_allow_html=True)

    rec_types = recs.get("by_type", {})
    if rec_types:
        color_map = {
            "switch_to_reserved": "#4F8EF7",
            "rightsizing":        "#22D3A0",
            "delete_unused":      "#EF4444",
        }
        bar_colors = [color_map.get(k, "#6B7A99") for k in rec_types.keys()]
        label_map  = {
            "switch_to_reserved": "Reserved",
            "rightsizing":        "Rightsize",
            "delete_unused":      "Delete",
        }
        labels = [label_map.get(k, k) for k in rec_types.keys()]

        fig = go.Figure(go.Bar(
            x=labels,
            y=list(rec_types.values()),
            marker=dict(color=bar_colors, line=dict(color="#080C14", width=1)),
            text=list(rec_types.values()),
            textposition="outside",
            textfont=dict(family="JetBrains Mono, monospace", size=11, color="#6B7A99"),
            hovertemplate="<b>%{x}</b><br>%{y} recommendation(s)<extra></extra>",
        ))
        _rec_layout = {k: v for k, v in CHART_LAYOUT.items() if k != "yaxis"}
        fig.update_layout(
            **_rec_layout,
            height=220,
            yaxis=dict(**{k: v for k, v in CHART_LAYOUT["yaxis"].items() if k != "tickprefix"},
                       tickprefix=""),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("""
        <div style="font-size:10px;color:#2D3F5A;font-family:'JetBrains Mono',monospace;line-height:2">
            <b style="color:#4F8EF7">Reserved</b> &mdash; Commit to 1yr/3yr for discount<br>
            <b style="color:#22D3A0">Rightsize</b> &mdash; Reduce over-provisioned instances<br>
            <b style="color:#EF4444">Delete</b> &mdash; Remove idle/unused resources
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#4A5568;font-size:12px;padding:40px 0;text-align:center;">No type data available</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ================= ALERTS + ANOMALY STATS =================
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    alert_items = alerts_resp.get("data", [])
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">Recent Alerts</div>
        <div class="section-badge">{len(alert_items)} active</div>
    </div>
    """, unsafe_allow_html=True)

    if alert_items:
        for item in alert_items:
            raw = str(item) if not isinstance(item, str) else item

            if "CRITICAL" in raw.upper():
                level, cls = "CRIT", "critical"
            elif "WARNING" in raw.upper() or "WARN" in raw.upper():
                level, cls = "WARN", "warning"
            else:
                level, cls = "INFO", "info"

            # Strip the severity tag from message to avoid duplication
            msg = raw.replace("[CRITICAL]", "").replace("[WARNING]", "").replace("[INFO]", "").strip()

            st.markdown(f"""
            <div class="alert-item {cls}">
                <span class="alert-severity">{level}</span>
                <span>{msg}</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#22D3A0;font-size:12px;padding:20px 0;text-align:center;">No active alerts</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="panel">', unsafe_allow_html=True)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">Anomaly Detection Stats</div>
        <div class="section-badge">Z-score model</div>
    </div>
    """, unsafe_allow_html=True)

    if anomaly_stats:
        count   = anomaly_stats.get("count", 0)
        mean_v  = float(anomaly_stats.get("mean", 0) or 0)
        std_v   = float(anomaly_stats.get("std_dev", 0) or 0)
        cv_val  = (std_v / mean_v * 100) if mean_v else 0
        upper2  = mean_v + 2 * std_v

        st.markdown(f"""
        <div class="anomaly-grid">
            <div class="anomaly-cell">
                <div class="ac-label">Data Points</div>
                <div class="ac-val">{count}</div>
                <div class="ac-context">Days analyzed</div>
            </div>
            <div class="anomaly-cell">
                <div class="ac-label">Mean Daily Cost</div>
                <div class="ac-val">{fmt_money(mean_v)}</div>
                <div class="ac-context">Baseline</div>
            </div>
            <div class="anomaly-cell">
                <div class="ac-label">Std Deviation</div>
                <div class="ac-val">{fmt_money(std_v)}</div>
                <div class="ac-context">CV: {cv_val:.1f}%</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Gauge for cost volatility
        st.markdown('<div style="margin-top:14px">', unsafe_allow_html=True)
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=cv_val,
            number=dict(suffix="%", font=dict(family="JetBrains Mono, monospace", size=22, color="#C9D1E0")),
            title=dict(text="Cost Volatility (CV)", font=dict(size=10, color="#4A5568", family="JetBrains Mono, monospace")),
            gauge=dict(
                axis=dict(range=[0, 60], tickcolor="#1A2236", tickfont=dict(size=9, color="#4A5568")),
                bar=dict(color="#4F8EF7"),
                bgcolor="#111827",
                borderwidth=0,
                steps=[
                    dict(range=[0, 15],  color="#0C1220"),
                    dict(range=[15, 30], color="rgba(245,158,11,0.1)"),
                    dict(range=[30, 60], color="rgba(239,68,68,0.1)"),
                ],
                threshold=dict(
                    line=dict(color="#EF4444", width=2),
                    thickness=0.75,
                    value=30
                ),
            )
        ))
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#6B7A99"),
            height=160,
            margin=dict(l=20, r=20, t=30, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(f'<div style="font-size:10px;color:#2D3F5A;font-family:JetBrains Mono,monospace;text-align:center">Anomaly threshold: {fmt_money(upper2)} (mean + 2σ)</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="color:#4A5568;font-size:12px;padding:40px 0;text-align:center;">No anomaly data available</div>', unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ================= REGIONAL COSTS =================
st.markdown('<div class="panel">', unsafe_allow_html=True)
region = region_data.get("service_cost_summary", {})
region_total = sum(region.values()) if region else 0
st.markdown(f"""
<div class="section-header">
    <div class="section-title">Regional Cost Breakdown &mdash; ap-south-1</div>
    <div class="section-badge">Total: {fmt_money(region_total)}</div>
</div>
""", unsafe_allow_html=True)

if region:
    reg_df = pd.DataFrame({
        "service": list(region.keys()),
        "cost": list(region.values()),
    }).sort_values("cost", ascending=False)

    reg_df["pct"] = reg_df["cost"] / region_total * 100

    fig = go.Figure()
    for idx, row in reg_df.iterrows():
        fig.add_trace(go.Bar(
            x=[row["service"]],
            y=[row["cost"]],
            name=row["service"],
            marker=dict(color=ACCENT_COLORS[list(reg_df.index).index(idx) % len(ACCENT_COLORS)]),
            text=[f"${row['cost']:,.0f}"],
            textposition="outside",
            textfont=dict(family="JetBrains Mono, monospace", size=11, color="#6B7A99"),
            hovertemplate=f"<b>{row['service']}</b><br>${row['cost']:,.2f}<br>{row['pct']:.1f}% of region<extra></extra>",
        ))

    fig.update_layout(
        **CHART_LAYOUT,
        height=260,
        showlegend=False,
        barmode="group",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Horizontal summary
    cols = st.columns(len(reg_df))
    for i, (_, row) in enumerate(reg_df.iterrows()):
        c = ACCENT_COLORS[i % len(ACCENT_COLORS)]
        with cols[i]:
            st.markdown(f"""
            <div style="text-align:center;padding:8px;background:#111827;border-radius:6px;border:1px solid #1A2236">
                <div style="font-size:10px;color:#4A5568;font-weight:700;letter-spacing:0.1em;text-transform:uppercase">{row['service']}</div>
                <div style="font-family:'JetBrains Mono',monospace;color:{c};font-size:15px;font-weight:600;margin:4px 0">{fmt_money(row['cost'])}</div>
                <div style="font-size:10px;color:#2D3F5A;font-family:'JetBrains Mono',monospace">{row['pct']:.1f}% of region</div>
            </div>
            """, unsafe_allow_html=True)
else:
    st.markdown('<div style="color:#4A5568;font-size:12px;padding:40px 0;text-align:center;">No regional data available for ap-south-1</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

st.markdown('<div class="panel">', unsafe_allow_html=True)
st.markdown("""
<div class="section-header">
    <div class="section-title">Query Explorer</div>
    <div class="section-badge">Live API Queries</div>
</div>
""", unsafe_allow_html=True)

query_type = st.selectbox(
    "Select Query",
    [
        "Cost by Date Range",
        "Usage by Service",
        "Usage by Region",
        "Top Accounts (Ranking)",
        "Anomaly Stats",
        "Recommendations"
    ]
)

# ---- Helper to show table ----
def show_table(result):
    if "data" not in result or not result["data"]:
        return

    data = result["data"]

    # Case 1: list of records (most queries)
    if isinstance(data, list):
        df = pd.DataFrame(data)

    # Case 2: single dict (anomaly, stats, etc.)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])

    else:
        return

    # Only select columns if they exist
    cols = [c for c in ["account_id", "resource_type", "region", "cost_usd", "usage_quantity"] if c in df.columns]
    if cols:
        df = df[cols]

    st.dataframe(df.head(10), use_container_width=True)


# ---- Cost Query ----
if query_type == "Cost by Date Range":
    start = st.date_input("Start Date")
    end   = st.date_input("End Date")

    if st.button("Run Cost Query"):
        result = safe_fetch(f"/costs/{selected_account}?start={start}&end={end}", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)
            show_table(result)
            st.json(result)


# ---- Service Query ----
elif query_type == "Usage by Service":
    service = st.selectbox("Service", ["EC2", "S3", "Lambda", "RDS", "CloudFront"])

    if st.button("Run Service Query"):
        result = safe_fetch(f"/usage/by-service/{service}", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)
            show_table(result)
            st.json(result)


# ---- Region Query ----
elif query_type == "Usage by Region":
    region = st.selectbox("Region", ["ap-south-1", "us-east-1", "eu-west-1"])

    if st.button("Run Region Query"):
        result = safe_fetch(f"/usage/by-region/{region}", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)
            show_table(result)
            st.json(result)


# ---- Rankings ----
elif query_type == "Top Accounts (Ranking)":
    if st.button("Run Ranking Query"):
        result = safe_fetch("/rankings", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)
            show_table(result)
            st.json(result)


# ---- Anomaly ----
elif query_type == "Anomaly Stats":
    if st.button("Run Anomaly Query"):
        result = safe_fetch(f"/anomaly-stats/{selected_account}", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)

            # Handle anomaly data properly
            if "data" in result and isinstance(result["data"], dict) and result["data"]:
                st.subheader("Anomaly Summary")
                st.json(result["data"])
            else:
                show_table(result)

            st.json(result)


# ---- Recommendations ----
elif query_type == "Recommendations":
    if st.button("Run Recommendation Query"):
        result = safe_fetch(f"/recommendations/{selected_account}", {"error": "No data"})

        if "error" in result:
            st.markdown('<div class="section-badge">Source: unavailable</div>', unsafe_allow_html=True)
            st.warning("No data returned")
        else:
            st.markdown(f'<div class="section-badge">Source: {result.get("source", "unknown")}</div>', unsafe_allow_html=True)
            show_table(result)
            st.json(result)

st.markdown('</div>', unsafe_allow_html=True)