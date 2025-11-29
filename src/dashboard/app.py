import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import PROCESSED_DATA_DIR

st.set_page_config(page_title="Chess Analytics", layout="wide", page_icon="♟️")

# Mapping for ECO Families
ECO_FAMILIES = {
    'A': 'A: Flank Openings',
    'B': 'B: Semi-Open Games',
    'C': 'C: Open Games',
    'D': 'D: Closed Games',
    'E': 'E: Indian Defenses'
}

@st.cache_data(ttl=10)  # Refresh cache every 10 seconds
def load_and_process_data():
    """
    Reads the raw processed games from Parquet and computes aggregations on-the-fly.
    This replaces the need for a separate aggregations.py batch script for the demo.
    """
    games_path = os.path.join(PROCESSED_DATA_DIR, "games")
    
    if not os.path.exists(games_path):
        return None

    try:
        # Read raw games directly (Spark writes partition folders, so we read the dir)
        # Using pyarrow for better performance with directory reading
        df = pd.read_parquet(games_path, engine='pyarrow')
        
        if df.empty:
            return None
            
        # --- Compute Aggregations In-Memory ---
        
        # 1. Comparison Metrics
        comparison = df.groupby('game_type').agg(
            total_games=('eco', 'count'),
            avg_moves=('num_moves', 'mean'),
            unique_ecos=('eco', 'nunique')
        ).reset_index()
        
        # 2. Opening Stats (Top ECOs)
        # Filter to top 20 per type to keep it light
        opening_stats = df.groupby(['game_type', 'eco', 'opening_family']).size().reset_index(name='count')
        opening_stats['family_name'] = opening_stats['opening_family'].map(ECO_FAMILIES).fillna('Unknown')
        
        # 3. Game Lengths (Just need the column)
        length_stats = df[['game_type', 'num_moves']].copy()
        
        # 4. Phase Stats (Endgame Rate)
        phase_stats = df.groupby(['game_type', 'game_phase']).size().reset_index(name='count')
        
        return {
            'comparison': comparison,
            'openings': opening_stats,
            'lengths': length_stats,
            'phases': phase_stats
        }
            
    except Exception as e:
        # This often happens if Spark is writing exactly when we try to read
        # We just ignore and wait for next refresh
        return None

def main():
    st.title("♟️ Professional vs Casual Chess Analytics")
    st.markdown("### Real-Time Streaming Dashboard")
    
    # Auto-refresh logic using a simple rerun button or loop if needed, 
    # but st.cache_data(ttl=...) coupled with a rerun trigger is standard.
    # For a simple auto-refresh feel without plugins:
    if st.button("Refresh Data 🔄"):
        st.rerun()
        
    # Load data
    data = load_and_process_data()
    
    if not data:
        st.warning("Waiting for data stream...")
        time.sleep(2)
        st.rerun()
        return

    # --- 1. OVERALL COMPARISON ---
    st.header("1. Overall Comparison")
    
    metrics = data['comparison'].set_index('game_type')
    
    # Create 3 columns for metrics
    c1, c2, c3 = st.columns(3)
    
    # Safe access helpers
    def get_val(df, idx, col):
        return df.loc[idx][col] if idx in df.index else 0

    casual_games = get_val(metrics, 'casual', 'total_games')
    pro_games = get_val(metrics, 'professional', 'total_games')
    
    with c1:
        st.metric("Total Games Analyzed", f"{casual_games + pro_games:,}")
        
    with c2:
        c_avg = get_val(metrics, 'casual', 'avg_moves')
        p_avg = get_val(metrics, 'professional', 'avg_moves')
        delta = p_avg - c_avg
        st.metric("Avg Moves (Pro)", f"{p_avg:.1f}", delta=f"{delta:.1f} vs Casual")
        
    with c3:
        c_eco = get_val(metrics, 'casual', 'unique_ecos')
        p_eco = get_val(metrics, 'professional', 'unique_ecos')
        st.metric("Unique ECO Codes (Pro)", f"{p_eco}", delta=f"{p_eco - c_eco} vs Casual")

    st.divider()

    # --- 2. GAME LENGTH DISTRIBUTION ---
    st.header("2. Game Length Distribution")
    st.caption("Are professional games longer than casual ones?")
    
    fig_len = px.histogram(
        data['lengths'], 
        x="num_moves", 
        color="game_type", 
        nbins=50,
        barmode="overlay",
        opacity=0.6,
        labels={"num_moves": "Number of Moves", "game_type": "Source"},
        color_discrete_map={"casual": "#00CC96", "professional": "#636EFA"}
    )
    fig_len.update_layout(xaxis_title="Moves", yaxis_title="Count")
    st.plotly_chart(fig_len, use_container_width=True)

    # --- 3. ENDGAME REACH RATE ---
    st.header("3. Endgame Reach Rate")
    
    # Calculate percentages
    phases = data['phases']
    total_per_type = phases.groupby('game_type')['count'].transform('sum')
    phases['percentage'] = (phases['count'] / total_per_type * 100).round(1)
    
    # Filter only Endgame
    endgame_data = phases[phases['game_phase'] == 'Endgame']
    
    if not endgame_data.empty:
        fig_phase = px.bar(
            endgame_data,
            x='game_type',
            y='percentage',
            color='game_type',
            text='percentage',
            title="Percentage of Games Reaching Endgame (>40 moves)",
            labels={'percentage': '% Reaching Endgame'},
            color_discrete_map={"casual": "#00CC96", "professional": "#636EFA"}
        )
        fig_phase.update_traces(texttemplate='%{text}%', textposition='outside')
        fig_phase.update_layout(yaxis_range=[0, 100])
        st.plotly_chart(fig_phase, use_container_width=True)

    st.divider()

    # --- 4. ECO FAMILY DISTRIBUTION ---
    st.header("4. Opening Families (ECO Categories)")
    st.caption("A: Flank | B: Semi-Open | C: Open | D: Closed | E: Indian")

    col1, col2 = st.columns(2)
    
    # Aggregate by family and type
    fam_stats = data['openings'].groupby(['game_type', 'family_name'])['count'].sum().reset_index()
    
    with col1:
        st.subheader("Casual Games")
        casual_fam = fam_stats[fam_stats['game_type'] == 'casual']
        if not casual_fam.empty:
            fig_pie1 = px.pie(casual_fam, values='count', names='family_name', hole=0.4)
            st.plotly_chart(fig_pie1, use_container_width=True)
        else:
            st.info("No casual data")

    with col2:
        st.subheader("Professional Games")
        pro_fam = fam_stats[fam_stats['game_type'] == 'professional']
        if not pro_fam.empty:
            fig_pie2 = px.pie(pro_fam, values='count', names='family_name', hole=0.4)
            st.plotly_chart(fig_pie2, use_container_width=True)
        else:
            st.info("No professional data")

    st.divider()

    # --- 5. TOP 10 SPECIFIC OPENINGS ---
    st.header("5. Top 10 Specific Openings (ECO Codes)")
    
    c_op, p_op = st.columns(2)
    
    ops = data['openings']
    
    with c_op:
        st.subheader("Casual: Top ECOs")
        top_casual = ops[ops['game_type'] == 'casual'].nlargest(10, 'count')
        if not top_casual.empty:
            fig_bar1 = px.bar(
                top_casual, x='count', y='eco', orientation='h',
                color='count', color_continuous_scale='Viridis'
            )
            fig_bar1.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar1, use_container_width=True)
    
    with p_op:
        st.subheader("Professional: Top ECOs")
        top_pro = ops[ops['game_type'] == 'professional'].nlargest(10, 'count')
        if not top_pro.empty:
            fig_bar2 = px.bar(
                top_pro, x='count', y='eco', orientation='h',
                color='count', color_continuous_scale='Magma'
            )
            fig_bar2.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar2, use_container_width=True)
            
    # Auto-refresh hack (re-runs script every 5 seconds)
    time.sleep(5)
    st.rerun()

if __name__ == "__main__":
    main()
