import streamlit as st
import pandas as pd
import altair as alt
import os
from datetime import timedelta
from config.settings import config

# --- Constants (Configuration) ---
# Move "Magic Numbers" here for easy adjustment
DATE_CUTOFF = "2024-01-01"
BP_Y_DOMAIN = [50, 180]  # Min/Max for Y-axis
CHART_HEIGHT = 350

# --- Configuration ---
st.set_page_config(
    page_title="Personal Data Platform", 
    page_icon="❤️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Authentication Logic ---
def render_sidebar_auth():
    """Renders login form. Returns True if authenticated."""
    if st.session_state.get("authenticated", False):
        if st.sidebar.button("🔒 Logout"):
            st.session_state["authenticated"] = False
            st.rerun()
        return True

    with st.sidebar.expander("🔐 Admin Access", expanded=False):
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            env_pass = os.getenv("DASHBOARD_PASSWORD")
            if env_pass and password == env_pass:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid Password")
    return False

# --- Data Loading ---
@st.cache_data
def load_data(show_real: bool):
    """Load data and apply hard cutoff for usability."""
    real_path = config.merged_path
    mock_path = config.MERGED_DATA_DIR / "mock_daily_metrics.csv"
    
    df = pd.DataFrame()
    source_type = "none"

    if show_real and real_path.exists():
        df = pd.read_csv(real_path, parse_dates=["date"])
        source_type = "real"
    elif mock_path.exists():
        df = pd.read_csv(mock_path, parse_dates=["date"])
        source_type = "mock"
    
    if not df.empty:
        # Apply Date Cutoff (defined in Constants)
        cutoff = pd.Timestamp(DATE_CUTOFF)
        df = df[df["date"] >= cutoff].copy()
        
        # Pre-calc hours
        df["sleep_hours"] = (df["total_duration"] / 60).round(1)

    return df, source_type

# --- Visualization Helper ---
def add_crosshair(base, main_layers, point_charts=None):
    """
    Adds a magnetic crosshair and hover dots to a chart.
    """
    # 1. The Selector (Invisible, tracks mouse)
    nearest = alt.selection_point(nearest=True, on='mouseover', fields=['date'], empty=False)
    
    selectors = alt.Chart(base.data).mark_point().encode(
        x='date:T',
        opacity=alt.value(0),
        # FIX: Disable tooltip on the selector so it doesn't block the real data
        tooltip=alt.value(None) 
    ).add_params(nearest)

    # 2. The Vertical Rule (Gray line)
    rule = alt.Chart(base.data).mark_rule(color='gray').encode(
        x='date:T',
        # Optional: Show date on the rule itself if no points are hovered
        tooltip=[alt.Tooltip('date', format='%Y-%m-%d')]
    ).transform_filter(nearest)

    # 3. The Hover Dots (Specific to each line)
    points_layers = []
    if point_charts:
        for chart in point_charts:
            # We clone the chart but change mark to circle and set opacity logic
            # The tooltip encoding is inherited from the original 'chart'
            pt = chart.mark_circle(size=60).encode(
                opacity=alt.condition(nearest, alt.value(1), alt.value(0))
            )
            points_layers.append(pt)

    # 4. Combine
    return alt.layer(main_layers, selectors, rule, *points_layers).interactive()

# --- Main Application ---
def main():
    is_authenticated = render_sidebar_auth()
    df, source_type = load_data(is_authenticated)
    
    if source_type == "none":
        st.error("No data found.")
        return

    # Header
    st.title("❤️ Personal Health Telemetry")
    med_start = pd.to_datetime(config.MED_START_DATE)
    
    if source_type == "real":
        st.success(f"Viewing **Real Data** (Authenticated). Medication Start: {med_start.date()}", icon="🔒")
    else:
        st.info("Viewing **Synthetic Demo Data**. (Login in sidebar for real data)", icon="🧪")

    # Filters
    st.sidebar.divider()
    st.sidebar.header("📅 Timeframe")
    
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    
    # Default: Last 3 months
    default_start = max(min_date, max_date - timedelta(days=90))
    
    date_range = st.sidebar.slider(
        "Select Range",
        min_value=min_date,
        max_value=max_date,
        value=(default_start, max_date)
    )

    mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])
    df_filtered = df.loc[mask].copy()

    # --- SHARED LAYERS ---
    med_rule = alt.Chart(pd.DataFrame({'date': [med_start]})).mark_rule(
        color='green', strokeDash=[5, 5], size=2
    ).encode(x='date:T')

    # --- CHART 1: BLOOD PRESSURE ---
    st.subheader("🩸 Blood Pressure")
    
    base_bp = alt.Chart(df_filtered).encode(x=alt.X('date:T', axis=alt.Axis(title=None)))
    
    # Define lines individually
    line_sys = base_bp.mark_line(color='#FF4B4B').encode(
        y=alt.Y('systolic', scale=alt.Scale(domain=BP_Y_DOMAIN), title='mmHg'),
        tooltip=[alt.Tooltip('date', format='%Y-%m-%d'), 'systolic', 'diastolic']
    )
    line_dia = base_bp.mark_line(color='#1C83E1').encode(y='diastolic')
    
    # Combine for display
    bp_layers = line_sys + line_dia + med_rule
    
    # Add interaction (Pass specific lines for dots)
    chart_bp = add_crosshair(base_bp, bp_layers, point_charts=[line_sys, line_dia])
    st.altair_chart(chart_bp.properties(height=CHART_HEIGHT), use_container_width=True)

    # --- CHART 2 & 3: SLEEP & HR ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💤 Sleep Duration")
        base_sleep = alt.Chart(df_filtered).encode(x=alt.X('date:T', axis=alt.Axis(title=None)))
        
        bar_sleep = base_sleep.mark_bar(color='#90EE90', opacity=0.8).encode(
            y=alt.Y('sleep_hours', title='Hours'),
            tooltip=[alt.Tooltip('date', format='%Y-%m-%d'), alt.Tooltip('sleep_hours', title='Hours')]
        )
        
        # Interaction (Bars don't usually need dots, just the rule)
        chart_sleep = add_crosshair(base_sleep, bar_sleep + med_rule)
        st.altair_chart(chart_sleep.properties(height=300), use_container_width=True)

    with col2:
        st.subheader("❤️ Resting Heart Rate")
        base_hr = alt.Chart(df_filtered).encode(x=alt.X('date:T', axis=alt.Axis(title=None)))
        
        line_hr = base_hr.mark_line(color='#FFA500').encode(
            y=alt.Y('min_hr', title='BPM', scale=alt.Scale(zero=False, padding=10)),
            tooltip=[alt.Tooltip('date', format='%Y-%m-%d'), alt.Tooltip('min_hr', title='Resting HR (BPM)')]
        )
        
        # Interaction (Pass line_hr for dots)
        chart_hr = add_crosshair(base_hr, line_hr + med_rule, point_charts=[line_hr])
        st.altair_chart(chart_hr.properties(height=300), use_container_width=True)

    # --- Footer ---
    st.markdown("---")
    st.caption(f"Pipeline Version: {config.ENV} | Source: {'Authenticated' if is_authenticated else 'Public Demo'}")

if __name__ == "__main__":
    main()