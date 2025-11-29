import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import time
from datetime import datetime

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
    """
    games_path = os.path.join(PROCESSED_DATA_DIR, "games")
    
    if not os.path.exists(games_path):
        return None

    try:
        # Read raw games
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
        opening_stats = df.groupby(['game_type', 'eco', 'opening_family']).size().reset_index(name='count')
        opening_stats['family_name'] = opening_stats['opening_family'].map(ECO_FAMILIES).fillna('Unknown')
        
        # 3. Game Lengths
        length_stats = df[['game_type', 'num_moves']].copy()
        
        # 4. Phase Stats
        phase_stats = df.groupby(['game_type', 'game_phase']).size().reset_index(name='count')
        
        return {
            'comparison': comparison,
            'openings': opening_stats,
            'lengths': length_stats,
            'phases': phase_stats
        }
            
    except Exception as e:
        return None

def update_history(comparison_df, data=None):
    """
    Updates the session state history with the latest metrics.
    """
    if 'history' not in st.session_state:
        st.session_state['history'] = []

    # Extract current metrics
    # Calculate percentages for endgame
    phases = data['phases']
    total_per_type = phases.groupby('game_type')['count'].transform('sum')
    phases['percentage'] = (phases['count'] / total_per_type * 100).round(1)
    endgame_rows = phases[phases['game_phase'] == 'Endgame']
    
    def get_endgame_pct(gtype):
        row = endgame_rows[endgame_rows['game_type'] == gtype]
        return row['percentage'].values[0] if not row.empty else 0.0

    current_stats = {
        'timestamp': datetime.now().strftime("%H:%M:%S"),
        'casual_total': 0,
        'pro_total': 0,
        'casual_eco': 0,
        'pro_eco': 0,
        'casual_avg': 0.0,
        'pro_avg': 0.0,
        'casual_endgame': 0.0,
        'pro_endgame': 0.0
    }
    
    if comparison_df is not None:
        # Helper to safely get value
        def get_val(df, gtype, col):
            row = df[df['game_type'] == gtype]
            return row[col].values[0] if not row.empty else 0

        current_stats['casual_total'] = int(get_val(comparison_df, 'casual', 'total_games'))
        current_stats['pro_total'] = int(get_val(comparison_df, 'professional', 'total_games'))
        current_stats['casual_eco'] = int(get_val(comparison_df, 'casual', 'unique_ecos'))
        current_stats['pro_eco'] = int(get_val(comparison_df, 'professional', 'unique_ecos'))
        current_stats['casual_avg'] = float(get_val(comparison_df, 'casual', 'avg_moves'))
        current_stats['pro_avg'] = float(get_val(comparison_df, 'professional', 'avg_moves'))
        current_stats['casual_endgame'] = get_endgame_pct('casual')
        current_stats['pro_endgame'] = get_endgame_pct('professional')

    # Only add if it's different from the last entry (to avoid duplicate rows on empty refreshes)
    if not st.session_state['history'] or \
       st.session_state['history'][-1]['casual_total'] != current_stats['casual_total'] or \
       st.session_state['history'][-1]['pro_total'] != current_stats['pro_total']:
        
        st.session_state['history'].append(current_stats)
        
        # Keep only last 5
        if len(st.session_state['history']) > 6:
             st.session_state['history'].pop(0)

def main():
    st.title("♟️ Professional vs Casual Chess Analytics")
    
    # Auto-refresh button
    if st.button("Refresh Data 🔄"):
        st.rerun()
        
    # Load data
    data = load_and_process_data()
    
    if not data:
        st.warning("Waiting for data stream...")
        time.sleep(2)
        st.rerun()
        return

    # Update History
    update_history(data['comparison'], data)

    # --- 0. LIVE INGESTION LOG (New Feature) ---
    st.subheader("📊 Live Ingestion Log (Last 5 Batches)")
    
    if 'history' in st.session_state and len(st.session_state['history']) > 1:
        # Calculate Deltas
        history = st.session_state['history']
        log_data = []
        
        # We iterate backwards to show newest first
        for i in range(len(history)-1, 0, -1):
            curr = history[i]
            prev = history[i-1]
            
            new_casual = curr['casual_total'] - prev['casual_total']
            new_pro = curr['pro_total'] - prev['pro_total']
            
            # Skip if no new games
            if new_casual == 0 and new_pro == 0:
                continue
            
            eco_change = (curr['casual_eco'] + curr['pro_eco']) - (prev['casual_eco'] + prev['pro_eco'])
            len_change = (curr['casual_avg'] - prev['casual_avg']) # Just showing drift in casual avg for brevity or combine
            endgame_change = (curr['casual_endgame'] - prev['casual_endgame'])

            log_data.append({
                "Time": curr['timestamp'],
                "New Casual": f"+{new_casual}",
                "New Pro": f"+{new_pro}",
                "Unique ECO Δ": f"{eco_change:+d}",
                "Game Len Δ (Cas)": f"{len_change:+.2f}",
                "Endgame Rate Δ (Cas)": f"{endgame_change:+.2f}%"
            })
            
        if log_data:
            st.dataframe(pd.DataFrame(log_data), use_container_width=True)
        else:
            st.info("No new data in recent refreshes.")
    else:
        st.info("Waiting for more batches to calculate deltas...")

    st.divider()

    # --- 1. OVERALL COMPARISON ---
    st.header("1. Overall Comparison")
    
    metrics = data['comparison'].set_index('game_type')
    c1, c2, c3 = st.columns(3)
    
    def get_val(df, idx, col):
        return df.loc[idx][col] if idx in df.index else 0

    casual_games = get_val(metrics, 'casual', 'total_games')
    pro_games = get_val(metrics, 'professional', 'total_games')
    
    # Calculate total delta for the top card using history if available
    delta_games = 0
    if 'history' in st.session_state and len(st.session_state['history']) > 1:
         latest = st.session_state['history'][-1]
         prev = st.session_state['history'][-2]
         delta_games = (latest['casual_total'] + latest['pro_total']) - (prev['casual_total'] + prev['pro_total'])

    with c1:
        st.metric("Total Games Analyzed", f"{casual_games + pro_games:,}", delta=f"+{delta_games} since last refresh")
        
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
    st.plotly_chart(fig_len, use_container_width=True)

    # --- 3. ENDGAME REACH RATE ---
    st.header("3. Endgame Reach Rate")
    
    phases = data['phases']
    total_per_type = phases.groupby('game_type')['count'].transform('sum')
    phases['percentage'] = (phases['count'] / total_per_type * 100).round(1)
    
    endgame_data = phases[phases['game_phase'] == 'Endgame']
    
    if not endgame_data.empty:
        fig_phase = px.bar(
            endgame_data,
            x='game_type',
            y='percentage',
            color='game_type',
            text='percentage',
            title="Percentage of Games Reaching Endgame (>40 moves)",
            color_discrete_map={"casual": "#00CC96", "professional": "#636EFA"}
        )
        fig_phase.update_traces(texttemplate='%{text}%', textposition='outside')
        st.plotly_chart(fig_phase, use_container_width=True)

    st.divider()

    # --- 4. OPENING ANALYSIS ---
    c_op, p_op = st.columns(2)
    
    ops = data['openings']
    
    with c_op:
        st.subheader("Casual: Top ECOs")
        top_casual = ops[ops['game_type'] == 'casual'].nlargest(10, 'count')
        if not top_casual.empty:
            fig_bar1 = px.bar(top_casual, x='count', y='eco', orientation='h', color='count')
            fig_bar1.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar1, use_container_width=True)
    
    with p_op:
        st.subheader("Professional: Top ECOs")
        top_pro = ops[ops['game_type'] == 'professional'].nlargest(10, 'count')
        if not top_pro.empty:
            fig_bar2 = px.bar(top_pro, x='count', y='eco', orientation='h', color='count')
            fig_bar2.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar2, use_container_width=True)
            
    # Auto-refresh
    time.sleep(5)
    st.rerun()

if __name__ == "__main__":
    main()
