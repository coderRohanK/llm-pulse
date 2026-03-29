import streamlit as st
from databricks import sql
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ── Page config ────────────────────────────────────────────
st.set_page_config(
    page_title = "LLM Pulse",
    page_icon  = "⚡",
    layout     = "wide"
)

# ── Databricks SQL connection ──────────────────────────────
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname = os.getenv("DATABRICKS_HOST"),
        http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
        access_token    = os.getenv("DATABRICKS_TOKEN")
    )

@st.cache_data(ttl=300)   # cache for 5 minutes
def run_query(query):
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Sidebar navigation ─────────────────────────────────────
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png", width=160)
st.sidebar.markdown("## ⚡ LLM Pulse")
st.sidebar.markdown("LLM API Observability Platform")
st.sidebar.divider()

view = st.sidebar.radio(
    "Navigate",
    ["Finance View", "Engineering View", "Quality View", "Alerts"],
    index = 0
)

st.sidebar.divider()
team_filter = st.sidebar.multiselect(
    "Filter by team",
    ["product", "data", "marketing", "ops", "finance"],
    default = ["product", "data", "marketing", "ops", "finance"]
)

# ── Helper: format team filter for SQL ────────────────────
def team_sql(col="team"):
    teams = "', '".join(team_filter)
    return f"{col} IN ('{teams}')"

# ══════════════════════════════════════════════════════════
# FINANCE VIEW
# ══════════════════════════════════════════════════════════
if view == "Finance View":
    st.title("💰 Finance View — LLM Cost Intelligence")

    # ── Top KPI counters ──────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    total_spend = run_query(f"""
        SELECT ROUND(SUM(total_cost_usd), 2) AS val
        FROM llm_pulse_dev.gold.daily_cost_by_team
        WHERE {team_sql()}
    """)
    total_calls = run_query(f"""
        SELECT SUM(total_calls) AS val
        FROM llm_pulse_dev.gold.daily_cost_by_team
        WHERE {team_sql()}
    """)
    avg_cost = run_query(f"""
        SELECT ROUND(AVG(avg_cost_per_call), 6) AS val
        FROM llm_pulse_dev.gold.daily_cost_by_team
        WHERE {team_sql()}
    """)
    forecast_total = run_query(f"""
        SELECT ROUND(SUM(predicted_cost_usd), 2) AS val
        FROM llm_pulse_dev.gold.cost_predictions
        WHERE {team_sql()}
        AND prediction_horizon_days <= 30
    """)

    col1.metric("Total spend (all time)", f"${total_spend['val'].iloc[0]:,.2f}")
    col2.metric("Total API calls",        f"{int(total_calls['val'].iloc[0]):,}")
    col3.metric("Avg cost per call",      f"${avg_cost['val'].iloc[0]:.6f}")
    col4.metric("30-day forecast",        f"${forecast_total['val'].iloc[0]:,.2f}")

    st.divider()

    # ── Daily cost trend ──────────────────────────────────
    st.subheader("Daily spend by team — 7-day rolling average")
    cost_trend = run_query(f"""
        SELECT event_date, team, rolling_7d_avg_cost, total_cost_usd, total_calls
        FROM llm_pulse_dev.gold.daily_cost_by_team
        WHERE {team_sql()}
        ORDER BY event_date
    """)
    cost_trend['event_date'] = pd.to_datetime(cost_trend['event_date'])

    fig = px.line(
        cost_trend, x='event_date', y='rolling_7d_avg_cost',
        color='team', title='7-day rolling average cost per team',
        labels={'rolling_7d_avg_cost':'Cost (USD)','event_date':'Date'}
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Cost by model ─────────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Total spend by model")
        model_cost = run_query("""
            SELECT model,
                   ROUND(SUM(total_cost_usd), 2) AS total_spend
            FROM llm_pulse_dev.gold.model_performance_summary
            GROUP BY model ORDER BY total_spend DESC
        """)
        fig2 = px.bar(model_cost, x='model', y='total_spend',
                      color='model', title='Total spend by model (USD)')
        st.plotly_chart(fig2, use_container_width=True)

    with col_b:
        st.subheader("30-day cost forecast")
        forecast = run_query(f"""
            SELECT prediction_date, team, predicted_cost_usd
            FROM llm_pulse_dev.gold.cost_predictions
            WHERE {team_sql()}
            ORDER BY prediction_date
        """)
        forecast['prediction_date'] = pd.to_datetime(forecast['prediction_date'])
        fig3 = px.line(forecast, x='prediction_date', y='predicted_cost_usd',
                       color='team', title='ML-predicted cost next 30 days')
        st.plotly_chart(fig3, use_container_width=True)

    # ── Feature breakdown table ───────────────────────────
    st.subheader("Most expensive features")
    features = run_query(f"""
        SELECT feature, team,
               ROUND(SUM(total_cost_usd), 2) AS total_spend,
               SUM(total_calls)              AS total_calls
        FROM llm_pulse_dev.gold.feature_cost_breakdown
        WHERE {team_sql()}
        GROUP BY feature, team
        ORDER BY total_spend DESC
        LIMIT 10
    """)
    st.dataframe(features, use_container_width=True)

# ══════════════════════════════════════════════════════════
# ENGINEERING VIEW
# ══════════════════════════════════════════════════════════
elif view == "Engineering View":
    st.title("⚙️ Engineering View — Model Performance")

    perf = run_query("""
        SELECT model,
               ROUND(AVG(avg_latency_ms), 0)    AS avg_latency_ms,
               ROUND(AVG(error_rate_pct), 2)     AS avg_error_rate_pct,
               ROUND(AVG(slow_call_rate_pct), 2) AS avg_slow_call_pct,
               SUM(total_calls)                  AS total_calls,
               ROUND(SUM(total_cost_usd), 2)     AS total_cost_usd
        FROM llm_pulse_dev.gold.model_performance_summary
        GROUP BY model
    """)

    col1, col2, col3 = st.columns(3)
    for i, row in perf.iterrows():
        with [col1, col2, col3][i]:
            st.markdown(f"### {row['model']}")
            st.metric("Avg latency",   f"{row['avg_latency_ms']:.0f} ms")
            st.metric("Error rate",    f"{row['avg_error_rate_pct']:.2f}%")
            st.metric("Slow call rate",f"{row['avg_slow_call_pct']:.2f}%")
            st.metric("Total cost",    f"${row['total_cost_usd']:,.2f}")

    st.divider()

    # ── Latency trend ────────────────────────────────────
    st.subheader("Latency trend per model")
    latency = run_query("""
        SELECT event_date, model,
               ROUND(avg_latency_ms, 0) AS avg_latency_ms
        FROM llm_pulse_dev.gold.model_performance_summary
        ORDER BY event_date
    """)
    latency['event_date'] = pd.to_datetime(latency['event_date'])
    fig = px.line(latency, x='event_date', y='avg_latency_ms',
                  color='model', title='Average latency per model over time (ms)')
    st.plotly_chart(fig, use_container_width=True)

    # ── Error rate trend ─────────────────────────────────
    st.subheader("Error rate trend per model")
    errors = run_query("""
        SELECT event_date, model,
               ROUND(error_rate_pct, 2) AS error_rate_pct
        FROM llm_pulse_dev.gold.model_performance_summary
        ORDER BY event_date
    """)
    errors['event_date'] = pd.to_datetime(errors['event_date'])
    fig2 = px.line(errors, x='event_date', y='error_rate_pct',
                   color='model', title='Daily error rate % per model')
    st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════
# QUALITY VIEW
# ══════════════════════════════════════════════════════════
elif view == "Quality View":
    st.title("✅ Quality View — Model Output Quality")

    quality = run_query("""
        SELECT feedback_date, model,
               quality_score_pct,
               rolling_7d_quality_score
        FROM llm_pulse_dev.gold.quality_score_daily
        ORDER BY feedback_date
    """)
    quality['feedback_date'] = pd.to_datetime(quality['feedback_date'])

    # Latest quality scores
    latest = quality.sort_values('feedback_date').groupby('model').last().reset_index()
    cols = st.columns(len(latest))
    for i, row in latest.iterrows():
        delta_color = "normal" if row['quality_score_pct'] >= 85 else "inverse"
        cols[i].metric(
            row['model'],
            f"{row['quality_score_pct']:.1f}%",
            delta = f"7d avg: {row['rolling_7d_quality_score']:.1f}%"
        )

    st.divider()
    st.subheader("Quality score trend (7-day rolling average)")
    fig = px.line(quality, x='feedback_date', y='rolling_7d_quality_score',
                  color='model',
                  title='Rolling 7-day quality score per model (%)',
                  labels={'rolling_7d_quality_score':'Quality score (%)'})
    fig.add_hline(y=85, line_dash="dash", line_color="orange",
                  annotation_text="Warning threshold (85%)")
    fig.add_hline(y=75, line_dash="dash", line_color="red",
                  annotation_text="Critical threshold (75%)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Daily quality scores")
    fig2 = px.line(quality, x='feedback_date', y='quality_score_pct',
                   color='model', title='Raw daily quality score per model')
    st.plotly_chart(fig2, use_container_width=True)

# ══════════════════════════════════════════════════════════
# ALERTS VIEW
# ══════════════════════════════════════════════════════════
elif view == "Alerts":
    st.title("🚨 Alerts — Cost Spikes & Quality Drops")

    alerts = run_query("""
        SELECT alert_date, team, model,
               alert_type, alert_severity,
               actual_value, threshold_value,
               ROUND(actual_value - threshold_value, 4) AS deviation
        FROM llm_pulse_dev.gold.quality_alerts
        ORDER BY alert_date DESC
    """)
    alerts['alert_date'] = pd.to_datetime(alerts['alert_date'])

    col1, col2, col3 = st.columns(3)
    critical = alerts[alerts['alert_severity']=='critical']
    warning  = alerts[alerts['alert_severity']=='warning']
    col1.metric("Total alerts",    len(alerts))
    col2.metric("Critical alerts", len(critical), delta=f"{len(critical)} need action", delta_color="inverse")
    col3.metric("Warning alerts",  len(warning))

    st.divider()

    severity_filter = st.selectbox(
        "Filter by severity", ["All", "critical", "warning"])
    type_filter = st.selectbox(
        "Filter by type", ["All", "cost_spike", "quality_drop"])

    filtered = alerts.copy()
    if severity_filter != "All":
        filtered = filtered[filtered['alert_severity'] == severity_filter]
    if type_filter != "All":
        filtered = filtered[filtered['alert_type'] == type_filter]

    st.dataframe(
        filtered.style.applymap(
            lambda v: 'color: red; font-weight: bold'
            if v == 'critical' else '',
            subset=['alert_severity']
        ),
        use_container_width=True
    )
