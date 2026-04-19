import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
from dotenv import load_dotenv
import os

# Load credentials
load_dotenv()

# Setup database connection
DB_URL = f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/{os.getenv('POSTGRES_DB')}"
engine = create_engine(DB_URL)

# --- App Layout ---
st.set_page_config(page_title="Premier League Insights", layout="wide")

st.title("🏆 Premier League Insights Dashboard")
st.markdown("""
Welcome to your automated Data Engineering project! This dashboard pulls data directly from 
the **PostgreSQL Gold Layer** transformed by dbt.
""")

# --- Data Fetching ---
@st.cache_data(ttl=600)  # Cache for 10 minutes to save DB resources
def get_league_table():
    query = "SELECT * FROM fct_league_table"
    return pd.read_sql(query, engine)

try:
    df = get_league_table()

    # --- Metrics Section ---
    st.divider()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        leader = df.iloc[0]['team_name']
        st.metric("Current Leader", leader)
    
    with col2:
        total_goals = df['gf'].sum()
        st.metric("Total Goals Scored", f"{int(total_goals)}")
        
    with col3:
        total_teams = len(df)
        st.metric("Teams in Database", total_teams)

    # --- Dashboard Content ---
    st.divider()
    
    tab1, tab2 = st.tabs(["📊 Standings Table", "📈 Statistics Charts"])

    with tab1:
        st.subheader("Current League Table")
        # Apply some styling to the table
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Performance Analysis")
        
        c1, c2 = st.columns(2)
        
        with c1:
            st.write("#### Total Points by Team")
            fig_points = px.bar(df, x='team_name', y='total_points', 
                               color='total_points', 
                               labels={'total_points': 'Points', 'team_name': 'Team'},
                               template="plotly_dark")
            st.plotly_chart(fig_points, use_container_width=True)
            
        with c2:
            st.write("#### Goal Difference Analysis")
            fig_gd = px.scatter(df, x='gf', y='ga', text='team_name',
                               size='total_points', color='gd',
                               labels={'gf': 'Goals For', 'ga': 'Goals Against'},
                               template="plotly_white")
            st.plotly_chart(fig_gd, use_container_width=True)

    # --- Footer ---
    st.sidebar.info("Data pipeline: API -> Postgres (Raw) -> dbt (Staging) -> dbt (Marts) -> Streamlit")
    st.sidebar.write(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Ensure that Docker containers are running and dbt has been initialized.")
