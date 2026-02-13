import streamlit as st
import pandas as pd
import altair as alt
import os
from datetime import timedelta
from config.settings import config

# --- Configuration ---
st.set_page_config(page_title="Personal Data Platform", page_icon="❤️", layout="wide")

# --- Authentication Logic ---
def check_password():
    """Returns True if the user is authenticated."""
    # If no password set in env, assume open access (or demo mode)
    if not os.getenv("DASHBOARD_PASSWORD"):
        return True

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if st.session_state["authenticated"]:
        return True

    # Login Form
    st.title("🔒 Login")
    password = st.text_input("Enter Password", type="password")
    if st.button("Login"):
        if password == os.getenv("DASHBOARD_PASSWORD"):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password")
    return False

# --- Data Loading ---
@st.cache_data
def load_data(is_demo: bool):
    """Load data based on mode."""
    real_path = config.merged_path
    mock_path = config.MERGED_DATA_DIR / "mock_daily_metrics.csv"
    
    if not is_demo and real_path.exists():
        return pd.read_csv(real_path, parse_dates=["date"]), "real"
    
    if mock_path.exists():
        return pd.read_csv(mock_path, parse_dates=["date"]), "mock"
    
    return pd.DataFrame(), "none"

# --- Main Application ---
def main():
    # 1. Handle Authentication
    # If DEMO_MODE is true, we skip auth and show mock data
    is_demo = os.getenv("DEMO_MODE", "false").lower() == "true"
    
    if not is_demo:
        if not check_password():
            return # Stop execution if not logged in

    # 2. Load Data
    df, source_type = load_data(is_demo)
    
    if source_type == "none":
        st.error("No data found.")
        return
    
    # UI Header
    st.title("❤️ Personal Health Telemetry")
    if source_type == "real":
        st.toast("Authenticated: Live Data", icon="🔒")
    else:
        st.toast("Public Demo Mode", icon="🧪")

    # 3. Pre-processing for Visualization
    # Medication Start Logic
    med_start = pd.to_datetime(config.MED_START_DATE)
    
    # Sidebar: Date Filter relative to Medication
    st.sidebar.header("Analysis Settings")
    
    # Default: Show 6 months before meds to today
    default_start = med_start - timedelta(days=180)
    min_date = df["date"].min()
    max_date = df["date"].max()
    
    # Ensure defaults are within data bounds
    default_start = max(default_start, min_date)
    
    date_range = st.sidebar.slider(
        "Timeframe",
        min_value=min_date.date(),
        max_value=max_date.date(),
        value=(default_start.date(), max_date.date())
    )

    # Filter Data
    mask = (df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])
    df_filtered = df.loc[mask].copy()

    # 4. Advanced Visualization (Altair)
    st.subheader("🩸 Blood Pressure & Medication Impact")
    
    # Base Chart
    base = alt.Chart(df_filtered).encode(x='date:T')

    # Lines for Systolic/Diastolic
    sys_line = base.mark_line(color='#FF4B4B').encode(y='systolic', tooltip=['date', 'systolic'])
    dia_line = base.mark_line(color='#1C83E1').encode(y='diastolic', tooltip=['date', 'diastolic'])
    
    # Medication Start Rule (Vertical Line)
    med_rule = alt.Chart(pd.DataFrame({'date': [med_start]})).mark_rule(
        color='green', strokeDash=[5, 5], size=2
    ).encode(x='date:T')
    
    # Annotation Text
    med_text = alt.Chart(pd.DataFrame({
        'date': [med_start], 
        'y': [140], 
        'text': ['Medication Start']
    })).mark_text(align='left', dx=5, color='green').encode(x='date:T', y='y', text='text')

    # Combine
    chart = (sys_line + dia_line + med_rule + med_text).interactive()
    st.altair_chart(chart, use_container_width=True)

    # 5. Context / Comments (If available)
    # Assuming you might have a 'notes' column in the future
    if 'context_notes' in df_filtered.columns:
        st.subheader("📝 Context Notes")
        notes_df = df_filtered[df_filtered['context_notes'].notna()][['date', 'context_notes', 'systolic', 'diastolic']]
        st.dataframe(notes_df, hide_index=True)

    # 6. Sleep vs HR
    st.subheader("💤 Sleep & Recovery")
    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(df_filtered, x="date", y="total_duration", color="#90EE90")
    with col2:
        st.line_chart(df_filtered, x="date", y="min_hr", color="#FFA500")

if __name__ == "__main__":
    main()