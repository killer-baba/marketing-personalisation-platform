import sqlite3
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from src.utils.config import SQLITE_PATH

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Marketing Personalisation Platform",
    page_icon="🎯",
    layout="wide",
)

API_BASE = "http://localhost:8000"

# ── DB helper ─────────────────────────────────────────────────
def get_conn():
    if not Path(SQLITE_PATH).exists():
        st.error("SQLite database not found. Run the pipeline first.")
        st.stop()
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ══════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════
st.title("🎯 Marketing Personalisation Platform")
st.caption("Real-time monitoring dashboard — MongoDB · Milvus · Neo4j · SQLite · Redis")
st.divider()


# ══════════════════════════════════════════════════════════════
#  ROW 1 — KPI cards
# ══════════════════════════════════════════════════════════════
conn = get_conn()

total_runs    = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
total_ok      = conn.execute("SELECT SUM(records_ok)  FROM pipeline_runs").fetchone()[0] or 0
total_dlq     = conn.execute("SELECT SUM(records_dlq) FROM pipeline_runs").fetchone()[0] or 0
total_campaigns = conn.execute("SELECT COUNT(DISTINCT campaign_id) FROM user_interaction_summary").fetchone()[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Pipeline Runs",       total_runs)
c2.metric("Records Processed",   total_ok)
c3.metric("DLQ Records",         total_dlq,  delta=f"-{total_dlq} failed", delta_color="inverse")
c4.metric("Active Campaigns",    total_campaigns)

st.divider()


# ══════════════════════════════════════════════════════════════
#  ROW 2 — Pipeline run history  |  Top campaigns
# ══════════════════════════════════════════════════════════════
left, right = st.columns(2)

# ── Pipeline run history ──────────────────────────────────────
with left:
    st.subheader("📋 Pipeline Run History")

    runs = conn.execute("""
        SELECT
            run_id,
            started_at,
            finished_at,
            records_in,
            records_ok,
            records_dlq,
            status
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10
    """).fetchall()

    if runs:
        df_runs = pd.DataFrame([dict(r) for r in runs])

        # Shorten run_id for display
        df_runs["run_id"] = df_runs["run_id"].str[:8] + "..."

        # Colour status column
        def colour_status(val):
            if val == "success":
                return "background-color: #d4edda; color: #155724"
            elif val == "partial":
                return "background-color: #fff3cd; color: #856404"
            else:
                return "background-color: #f8d7da; color: #721c24"

        styled = df_runs.style.applymap(colour_status, subset=["status"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("No pipeline runs yet. Run the pipeline first.")

# ── Top campaigns by engagement ───────────────────────────────
with right:
    st.subheader("🏆 Top Campaigns by Engagement")

    campaigns = conn.execute("""
        SELECT
            campaign_id,
            SUM(interaction_count) AS total_interactions,
            COUNT(DISTINCT user_id) AS unique_users
        FROM user_interaction_summary
        GROUP BY campaign_id
        ORDER BY total_interactions DESC
        LIMIT 10
    """).fetchall()

    if campaigns:
        df_camp = pd.DataFrame([dict(c) for c in campaigns])

        # Bar chart
        st.bar_chart(
            df_camp.set_index("campaign_id")["total_interactions"],
            use_container_width=True,
            height=200,
        )

        # Table below chart
        st.dataframe(df_camp, use_container_width=True, hide_index=True)
    else:
        st.info("No campaign data yet. Run the pipeline first.")

st.divider()


# ══════════════════════════════════════════════════════════════
#  ROW 3 — Live recommendation demo
# ══════════════════════════════════════════════════════════════
st.subheader("🔍 Live Recommendation Engine")
st.caption("Enter a user ID to query the recommendation API in real time")

# Get known user_ids from SQLite for the dropdown
known_users = conn.execute("""
    SELECT DISTINCT user_id
    FROM user_interaction_summary
    ORDER BY user_id
""").fetchall()
known_user_ids = [r["user_id"] for r in known_users]

col_input, col_btn = st.columns([3, 1])

with col_input:
    # Dropdown of known users + option to type custom
    user_id_input = st.selectbox(
        "Select or type a user ID",
        options=[""] + known_user_ids,
        index=0,
    )
    custom_user = st.text_input("Or type a custom user ID", placeholder="e.g. u001")
    user_id = custom_user.strip() if custom_user.strip() else user_id_input

with col_btn:
    st.write("")  # spacer
    st.write("")  # spacer
    get_reco = st.button("Get Recommendations", type="primary", use_container_width=True)
    clear_cache = st.button("Clear Cache (force refresh)", use_container_width=True)

# ── Clear cache call ──────────────────────────────────────────
if clear_cache and user_id:
    try:
        from src.db import redis_client
        redis_client.invalidate_user(user_id)
        st.success(f"Cache cleared for {user_id}. Next call will be a fresh lookup.")
    except Exception as e:
        st.error(f"Could not clear cache: {e}")

# ── Recommendation call ───────────────────────────────────────
if get_reco and user_id:
    with st.spinner(f"Fetching recommendations for {user_id}..."):
        try:
            resp = requests.get(
                f"{API_BASE}/recommendations/{user_id}",
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()

                # ── Latency breakdown ─────────────────────────
                st.write("")
                cache_label = "✅ Cache HIT" if data["cache_hit"] else "🔄 Cache MISS"
                total_ms    = round(data["total_ms"], 1)

                m1, m2, m3 = st.columns(3)
                m1.metric("Total Latency",  f"{total_ms} ms")
                m2.metric("Cache Status",   cache_label)
                m3.metric("Similar Users Found", len(data["similar_user_ids"]))

                # ── Latency breakdown bar (only on cache miss) ─
                if not data["cache_hit"] and len(data["latency_ms"]) > 1:
                    st.write("**Latency breakdown per stage:**")
                    lat = data["latency_ms"]
                    lat_display = {
                        k: round(v, 1)
                        for k, v in lat.items()
                        if k != "total_ms"
                    }
                    lat_df = pd.DataFrame(
                        lat_display.items(),
                        columns=["Stage", "Latency (ms)"]
                    )
                    st.bar_chart(
                        lat_df.set_index("Stage"),
                        use_container_width=True,
                        height=160,
                    )

                # ── Similar users ──────────────────────────────
                st.write("**Similar users found by Milvus:**")
                st.code(", ".join(data["similar_user_ids"]) or "none")

                # ── Recommendations table ──────────────────────
                st.write("**Recommended campaigns:**")
                if data["recommendations"]:
                    df_reco = pd.DataFrame(data["recommendations"])
                    df_reco["rank"] = df_reco["rank"].apply(lambda x: f"#{x}")
                    df_reco.columns = ["Campaign ID", "Engagement Score", "Graph Weight", "Rank"]
                    st.dataframe(
                        df_reco[["Rank", "Campaign ID", "Engagement Score", "Graph Weight"]],
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.warning(f"No recommendations found for user {user_id}. They may not have enough history yet.")

            else:
                st.error(f"API returned status {resp.status_code}: {resp.text}")

        except requests.exceptions.ConnectionError:
            st.error("Cannot connect to API. Make sure the FastAPI server is running on port 8000.")
        except Exception as e:
            st.error(f"Unexpected error: {e}")

elif get_reco and not user_id:
    st.warning("Please select or enter a user ID first.")

st.divider()

# ══════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════
st.caption(
    "Data sources: SQLite (metrics) · FastAPI (recommendations) · Redis (cache) | "
    "Refresh the page to reload metrics"
)