import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

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

@st.cache_data
def load_data():
    agg_path = os.path.join(PROCESSED_DATA_DIR, "aggregations")
    
    data = {}
    try:
        # Load all parquet files
        if os.path.exists(os.path.join(agg_path, "comparison_metrics")):
            data['comparison'] = pd.read_parquet(os.path.join(agg_path, "comparison_metrics"))
            
        if os.path.exists(os.path.join(agg_path, "opening_stats")):
            data['openings'] = pd.read_parquet(os.path.join(agg_path, "opening_stats"))
            # Map family names
            data['openings']['family_name'] = data['openings']['opening_family'].map(ECO_FAMILIES).fillna('Unknown')
            
        if os.path.exists(os.path.join(agg_path, "game_length_stats")):
            data['lengths'] = pd.read_parquet(os.path.join(agg_path, "game_length_stats"))

        if os.path.exists(os.path.join(agg_path, "phase_stats")):
            data['phases'] = pd.read_parquet(os.path.join(agg_path, "phase_stats"))
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None
    return data

def main():
    st.title("♟️ Professional vs Casual Chess Analytics")
    st.markdown("Comparing game patterns between **Lichess/Chess.com (Casual)** and **Professional Tournaments**.")
    
    data = load_data()
    if not data or 'comparison' not in data:
        st.warning("No data available. Please run the aggregation script first.")
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
    if 'lengths' in data:
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
    if 'phases' in data:
        st.header("3. Endgame Reach Rate")
        
        # Calculate percentages
        phases = data['phases']
        total_per_type = phases.groupby('game_type')['count'].transform('sum')
        phases['percentage'] = (phases['count'] / total_per_type * 100).round(1)
        
        # Filter only Endgame
        endgame_data = phases[phases['game_phase'] == 'Endgame']
        
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
    if 'openings' in data:
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
    if 'openings' in data:
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

if __name__ == "__main__":
    main()
