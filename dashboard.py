import streamlit as st
import pandas as pd
import os
from config.settings import config

# --- Configuration ---
st.set_page_config(
    page_title="Personal Data Platform", 
    page_icon="❤️",
    layout="wide"
)

# --- Data Loading Strategy ---
@st.cache_data
def load_data():
    """
    Load data based on environment context.
    Returns:
        Tuple[pd.DataFrame, str]: The data and the source type ('real', 'mock', or 'none')
    """
    real_path = config.merged_path
    mock_path = config.MERGED_DATA_DIR / "mock_daily_metrics.csv"
    
    # Check Environment Variable
    is_demo = os.getenv("DEMO_MODE", "false").lower() == "true"
    
    # Logic: Try Real -> Fallback to Mock -> Fail
    # We DO NOT call st.toast here anymore. We just return the data and a tag.
    if not is_demo and real_path.exists():
        return pd.read_csv(real_path, parse_dates=["date"]), "real"
    
    if mock_path.exists():
        return pd.read_csv(mock_path, parse_dates=["date"]), "mock"
    
    return pd.DataFrame(), "none"

# --- Main UI ---
def main():
    st.title("❤️ Personal Health Telemetry")
    st.markdown("""
    *Automated pipeline aggregating Blood Pressure (Google Sheets) and Sleep/HR (Mi Band).*
    *Built with Python, SQLite, and Docker.*
    """)

    # Load data and source type
    df, source_type = load_data()
    
    # Handle UI Notifications (Side Effects) here, outside the cache
    if source_type == "none":
        st.error("No data found. Please run the pipeline or mock generator.")
        return
    elif source_type == "real":
        st.toast("Loaded Real Data (Authenticated)", icon="🔒")
    elif source_type == "mock":
        st.toast("Loaded Mock Data (Demo Mode)", icon="🧪")

    # --- Sidebar Filters ---
    st.sidebar.header("Filters")
    
    # Date Range Slider
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    
    date_range = st.sidebar.slider(
        "Select Date Range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date)
    )

    # Filter Logic
    mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])
    df_filtered = df.loc[mask]

    # --- KPI Row ---
    col1, col2, col3, col4 = st.columns(4)
    
    avg_bp_sys = df_filtered['systolic'].mean()
    avg_bp_dia = df_filtered['diastolic'].mean()
    
    col1.metric("Avg BP", f"{avg_bp_sys:.0f} / {avg_bp_dia:.0f}", "mmHg")
    col2.metric("Avg Sleep", f"{df_filtered['total_duration'].mean()/60:.1f} hrs")
    col3.metric("Avg RHR", f"{df_filtered['min_hr'].mean():.0f} bpm")
    col4.metric("Data Points", len(df_filtered))

    # --- Charts ---
    st.divider()
    
    # 1. Blood Pressure Chart
    st.subheader("🩸 Blood Pressure Trends")
    st.line_chart(
        df_filtered, 
        x="date", 
        y=["systolic", "diastolic"],
        color=["#FF4B4B", "#1C83E1"] # Red/Blue
    )

    # 2. Sleep & Recovery Chart
    st.subheader("💤 Sleep Quality & Heart Rate")
    
    # Create a dual-axis chart concept using two columns
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.caption("Sleep Duration (Minutes)")
        st.bar_chart(df_filtered, x="date", y="total_duration", color="#90EE90")
        
    with chart_col2:
        st.caption("Resting Heart Rate")
        st.line_chart(df_filtered, x="date", y="min_hr", color="#FFA500")

    # --- Footer ---
    st.markdown("---")
    st.caption(f"Pipeline Version: {config.ENV} | Mode: {'Demo' if os.getenv('DEMO_MODE') == 'true' else 'Live'}")

if __name__ == "__main__":
    main()