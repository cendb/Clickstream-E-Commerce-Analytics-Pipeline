import streamlit as st
import duckdb
import pandas as pd
import os


# Set up page configuration
st.set_page_config(
    page_title="Clickstream Dashboard",
    page_icon="chart_with_upwards_trend",
    layout="wide"
)

# Dynamically find the absolute path of the directory this script is in
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Point to the data folder relative to this script
DB_PATH = os.path.join(SCRIPT_DIR, "..", "data", "warehouse.duckdb")

def get_dashboard_data():
    """Reads transformed Gold layer models from DuckDB."""
    try:
        # Open connection in read-only mode to allow concurrent dbt-runs/consumers
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Pull data from Gold models
        df_fact = con.execute("SELECT * FROM main.fact_clickstream").df()
        df_users = con.execute("SELECT * FROM main.dim_users").df()
        
        con.close()
        return df_fact, df_users
    except Exception as e:
        st.error(f"Waiting for database connection... {e}")
        return pd.DataFrame(), pd.DataFrame()

# Static Title Banner (This never moves, flashes, or reloads)
st.title("E-Commerce Clickstream Analytics")
st.markdown("Updates live by querying our transformed Gold-tier Medallion models in DuckDB.")
st.markdown("---")

# Define the fragment that auto-updates every 5 seconds in the background
@st.fragment(run_every=5)
def render_dynamic_dashboard():
    df_fact, df_users = get_dashboard_data()
    
    if not df_fact.empty and not df_users.empty:
        # 1. High-Level KPI Section
        col1, col2, col3, col4 = st.columns(4)
        
        total_events = len(df_fact)
        total_users = len(df_users)
        total_purchases = int(df_users['total_purchases'].sum())
        conversion_rate = (total_purchases / total_events) * 100 if total_events > 0 else 0.0
        
        col1.metric("Total Streamed Events", f"{total_events:,}")
        col2.metric("Active Users", f"{total_users:,}")
        col3.metric("Total Purchases Completed", f"{total_purchases:,}")
        col4.metric("Event-to-Purchase CR", f"{conversion_rate:.2f}%")
        
        st.markdown("---")
        
        # 2. Detail Charts Section
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Event Actions Distribution")
            event_counts = df_fact['event_type'].value_counts()
            # Added color parameter here
            st.bar_chart(event_counts, color="#4CAF50")
            
        with col_right:
            st.subheader("Device Usage Breakdown")
            device_counts = df_fact['device'].value_counts()
            # Added color parameter here
            st.bar_chart(device_counts, color="#04AD0C") # A slightly different forest green for contrast!
            
        # Sample of the most active users in the Gold layer
        st.subheader("Top 5 Most Active User Sessions")
        top_users = df_users.sort_values(by='total_interactions', ascending=False).head(5)
        st.dataframe(
            top_users[['user_id', 'total_interactions', 'total_purchases', 'preferred_device', 'last_active_at']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("No data found in warehouse.duckdb. Make sure your pipeline is running and dbt run has completed successfully.")

# Execute the dynamic fragment
render_dynamic_dashboard()