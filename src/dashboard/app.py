import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import time
import json

# Compatibility for older Streamlit versions
def rerun():
    """Compatible rerun function for different Streamlit versions"""
    if hasattr(st, 'rerun'):
        st.rerun()
    else:
        st.experimental_rerun()

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from config.settings import PROCESSED_DATA_DIR, DATA_DIR

st.set_page_config(page_title="Chess Analytics", layout="wide", page_icon="♟️")

# =========================================================================
# SPLASH SCREEN / INTRO ANIMATION
# =========================================================================
def show_splash_screen():
    """Display animated splash screen with chess theme"""
    
    # Hide default Streamlit elements for cleaner splash
    hide_streamlit = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
    """
    st.markdown(hide_streamlit, unsafe_allow_html=True)
    
    # Add spacing at top
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # Chess pieces - subtle, minimal animation
    st.markdown("""
    <div style="text-align: center; margin-bottom: 30px;">
        <span style="font-size: 3rem; color: #e8e8e8; margin: 0 5px;">♔</span>
        <span style="font-size: 3rem; color: #4a4a4a; margin: 0 5px;">♚</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Title - elegant cream/gold tones
    st.markdown("""
    <h1 style="text-align: center; font-size: 3.5rem; margin-bottom: 0; 
        color: #d4af37; font-family: Georgia, serif; font-weight: normal; letter-spacing: 8px;">
        CHESS ANALYTICS
    </h1>
    """, unsafe_allow_html=True)
    
    # Subtitle
    st.markdown("""
    <p style="text-align: center; font-size: 1rem; color: #6b6b6b; letter-spacing: 3px; margin-top: 10px; font-family: Georgia, serif;">
        REAL-TIME STREAMING DASHBOARD
    </p>
    """, unsafe_allow_html=True)
    
    # VS Badge - subtle dark with gold accent
    st.markdown("""
    <div style="text-align: center; margin: 30px 0;">
        <span style="background: #2a2a2a; border: 1px solid #3d3d3d;
            padding: 12px 30px; border-radius: 4px; color: #c9c9c9; font-size: 0.95rem; letter-spacing: 2px;">
             CASUAL &nbsp;&nbsp;<span style="color: #d4af37;">vs</span>&nbsp;&nbsp; PROFESSIONAL
        </span>
    </div>
    """, unsafe_allow_html=True)
    
    # Features in columns - minimal icons
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="text-align: center;">
            <div style="font-size: 1.5rem; color: #d4af37;">◉</div>
            <div style="color: #5a5a5a; font-size: 0.85rem; margin-top: 8px;">Live Streaming</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="text-align: center;">
            <div style="font-size: 1.5rem; color: #d4af37;">◉</div>
            <div style="color: #5a5a5a; font-size: 0.85rem; margin-top: 8px;">Spark Processing</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="text-align: center;">
            <div style="font-size: 1.5rem; color: #d4af37;">◉</div>
            <div style="color: #5a5a5a; font-size: 0.85rem; margin-top: 8px;">Analytics</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Subtle loading bar - gold accent
    st.markdown("""
    <div style="display: flex; justify-content: center; margin-top: 40px;">
        <div style="width: 200px; height: 2px; background: #2a2a2a; border-radius: 1px; overflow: hidden;">
            <div style="width: 100%; height: 100%; background: #d4af37;
                animation: loading 2s ease-in-out infinite;"></div>
        </div>
    </div>
    <style>
    @keyframes loading {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(100%); }
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Enter button - custom styled
    st.markdown("<br><br>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        # Custom button styling
        st.markdown("""
        <style>
        div.stButton > button {
            background: transparent;
            border: 1px solid #d4af37;
            color: #d4af37;
            padding: 12px 40px;
            font-size: 0.9rem;
            letter-spacing: 2px;
            transition: all 0.3s ease;
        }
        div.stButton > button:hover {
            background: #d4af37;
            color: #1a1a1a;
        }
        </style>
        """, unsafe_allow_html=True)
        if st.button("ENTER", use_container_width=True):
            st.session_state.show_dashboard = True
            rerun()

# Mapping for ECO Families
ECO_FAMILIES = {
    'A': 'A: Flank Openings',
    'B': 'B: Semi-Open Games',
    'C': 'C: Open Games',
    'D': 'D: Closed Games',
    'E': 'E: Indian Defenses'
}

# Color scheme
COLORS = {
    "casual": "#00CC96",      # Green
    "professional": "#636EFA"  # Blue
}

def check_streaming_complete():
    """Check if the producer has finished streaming all data"""
    completion_file = os.path.join(DATA_DIR, "streaming_complete.json")
    if os.path.exists(completion_file):
        try:
            with open(completion_file, 'r') as f:
                return json.load(f)
        except:
            return None
    return None

@st.cache_data(ttl=2)  # Refresh cache every 2 seconds for live updates
def load_and_process_data():
    """
    Reads the raw processed games from Parquet and computes aggregations on-the-fly.
    """
    games_path = os.path.join(PROCESSED_DATA_DIR, "games")
    
    if not os.path.exists(games_path):
        return None

    try:
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
        
        # 5. Game Results (Win/Draw/Loss)
        if 'result' in df.columns:
            result_stats = df.groupby(['game_type', 'result']).size().reset_index(name='count')
        else:
            result_stats = None
        
        # 6. Average moves by opening family
        avg_by_family = df.groupby(['game_type', 'opening_family']).agg(
            avg_moves=('num_moves', 'mean'),
            game_count=('eco', 'count')
        ).reset_index()
        avg_by_family['family_name'] = avg_by_family['opening_family'].map(ECO_FAMILIES).fillna('Unknown')
        
        # 7. Total counts for streaming stats
        total_count = len(df)
        casual_total = len(df[df['game_type'] == 'casual'])
        pro_total = len(df[df['game_type'] == 'professional'])
        
        return {
            'comparison': comparison,
            'openings': opening_stats,
            'lengths': length_stats,
            'phases': phase_stats,
            'results': result_stats,
            'avg_by_family': avg_by_family,
            'total_count': total_count,
            'casual_total': casual_total,
            'pro_total': pro_total
        }
            
    except Exception as e:
        return None

def main():
    # Initialize session state for splash screen
    if 'show_dashboard' not in st.session_state:
        st.session_state.show_dashboard = False
    
    # Show splash screen if not yet entered
    if not st.session_state.show_dashboard:
        show_splash_screen()
        return
    
    # =========================================================================
    # MAIN DASHBOARD (shown after clicking Enter)
    # =========================================================================
    st.title("♟️ Professional vs Casual Chess Analytics")
    st.markdown("### Real-Time Streaming Dashboard")
    
    # Check if streaming is complete
    completion_status = check_streaming_complete()
    
    # Load data first
    data = load_and_process_data()
    
    # Initialize session state for games/sec calculation
    if 'prev_count' not in st.session_state:
        st.session_state.prev_count = 0
    if 'prev_time' not in st.session_state:
        st.session_state.prev_time = time.time()
    
    # =========================================================================
    # LIVE STREAMING STATS (Replaces old table)
    # =========================================================================
    st.markdown("#### 📡 Live Streaming Stats")
    
    if data:
        current_count = data['total_count']
        current_time = time.time()
        
        # Calculate games per second
        time_diff = current_time - st.session_state.prev_time
        count_diff = current_count - st.session_state.prev_count
        
        if time_diff > 0 and count_diff >= 0:
            games_per_sec = count_diff / time_diff
        else:
            games_per_sec = 0
        
        # Update session state
        st.session_state.prev_count = current_count
        st.session_state.prev_time = current_time
        
        # Estimated total (based on typical file sizes - adjust as needed)
        ESTIMATED_TOTAL = 500000  # Approximate total games expected
        
        casual_count = data['casual_total']
        pro_count = data['pro_total']
        total_count = current_count
        
        # Display metrics in columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "⚡ Processing Speed",
                f"{games_per_sec:.0f} games/sec" if games_per_sec > 0 else "Calculating...",
                delta="streaming" if not completion_status else "complete"
            )
        
        with col2:
            casual_pct = (casual_count / total_count * 100) if total_count > 0 else 0
            st.metric(
                "🎮 Casual Games",
                f"{casual_count:,}",
                delta=f"{casual_pct:.1f}% of total"
            )
        
        with col3:
            pro_pct = (pro_count / total_count * 100) if total_count > 0 else 0
            st.metric(
                "🏆 Professional Games", 
                f"{pro_count:,}",
                delta=f"{pro_pct:.1f}% of total"
            )
        
        # Progress bars
        st.markdown("**Processing Progress**")
        
        # Overall progress
        progress_pct = min(total_count / ESTIMATED_TOTAL, 1.0)
        st.progress(progress_pct)
        
        # Show counts under progress bar
        prog_col1, prog_col2, prog_col3 = st.columns([1, 2, 1])
        with prog_col2:
            if completion_status:
                st.success(f"✅ **Complete!** {total_count:,} games processed")
            else:
                st.caption(f"📊 **{total_count:,}** games processed")
        
        # Casual vs Professional mini progress bars
        st.markdown("**Source Breakdown**")
        source_col1, source_col2 = st.columns(2)
        
        with source_col1:
            st.caption(f"Casual: {casual_count:,}")
            casual_progress = casual_count / max(casual_count + pro_count, 1)
            st.progress(casual_progress)
        
        with source_col2:
            st.caption(f"Professional: {pro_count:,}")
            pro_progress = pro_count / max(casual_count + pro_count, 1)
            st.progress(pro_progress)
    else:
        st.warning("⏳ Waiting for data stream to begin...")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("⚡ Processing Speed", "-- games/sec")
        with col2:
            st.metric("🎮 Casual Games", "0")
        with col3:
            st.metric("🏆 Professional Games", "0")
        st.progress(0)
    
    st.divider()
    
    if not data:
        st.warning("Waiting for data stream...")
        time.sleep(2)
        rerun()
        return

    # Safe access helper
    def get_val(df, idx, col):
        return df.loc[idx][col] if idx in df.index else 0

    metrics = data['comparison'].set_index('game_type')
    
    casual_games = get_val(metrics, 'casual', 'total_games')
    pro_games = get_val(metrics, 'professional', 'total_games')
    c_avg = get_val(metrics, 'casual', 'avg_moves')
    p_avg = get_val(metrics, 'professional', 'avg_moves')
    c_eco = get_val(metrics, 'casual', 'unique_ecos')
    p_eco = get_val(metrics, 'professional', 'unique_ecos')

    # =========================================================================
    # SECTION 1: OVERALL COMPARISON (IMPROVED - Side by Side)
    # =========================================================================
    st.header("1. Overall Comparison")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Total Games")
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            st.metric("Casual", f"{int(casual_games):,}", delta=None)
        with subcol2:
            st.metric("Professional", f"{int(pro_games):,}", delta=None)
        st.caption(f"**Total: {int(casual_games + pro_games):,}**")
    
    with col2:
        st.subheader("Avg Moves per Game")
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            st.metric("Casual", f"{c_avg:.1f}")
        with subcol2:
            delta = p_avg - c_avg
            st.metric("Professional", f"{p_avg:.1f}", delta=f"{delta:+.1f}")
    
    with col3:
        st.subheader("Unique ECO Codes")
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            st.metric("Casual", f"{int(c_eco)}")
        with subcol2:
            delta = int(p_eco - c_eco)
            st.metric("Professional", f"{int(p_eco)}", delta=f"{delta:+d}")

    st.divider()

    # =========================================================================
    # SECTION 2: GAME LENGTH DISTRIBUTION
    # =========================================================================
    st.header("2. Game Length Distribution")
    st.caption("How long are games in each category?")
    
    fig_len = px.histogram(
        data['lengths'], 
        x="num_moves", 
        color="game_type", 
        nbins=50,
        barmode="group",  # Side by side instead of overlay
        labels={"num_moves": "Number of Moves", "game_type": "Game Type"},
        color_discrete_map=COLORS
    )
    fig_len.update_layout(xaxis_title="Moves", yaxis_title="Count", bargap=0.1)
    st.plotly_chart(fig_len, use_container_width=True)

    st.divider()

    # =========================================================================
    # SECTION 3: GAME RESULTS DISTRIBUTION (NEW)
    # =========================================================================
    st.header("3. Game Results Distribution")
    st.caption("Win/Draw/Loss rates - Professional games typically have more draws")
    
    if data['results'] is not None and not data['results'].empty:
        results = data['results'].copy()
        
        # Calculate percentages
        total_per_type = results.groupby('game_type')['count'].transform('sum')
        results['percentage'] = (results['count'] / total_per_type * 100).round(1)
        
        # Clean up result labels
        def clean_result(r):
            if pd.isna(r) or r == '':
                return 'Unknown'
            r = str(r).strip()
            if r == '1-0':
                return 'White Wins'
            elif r == '0-1':
                return 'Black Wins'
            elif r == '1/2-1/2':
                return 'Draw'
            else:
                return 'Other'
        
        results['result_label'] = results['result'].apply(clean_result)
        
        # Aggregate by clean labels
        results_agg = results.groupby(['game_type', 'result_label']).agg(
            count=('count', 'sum'),
            percentage=('percentage', 'sum')
        ).reset_index()
        
        # Filter out Unknown/Other if they're small
        results_agg = results_agg[results_agg['result_label'].isin(['White Wins', 'Black Wins', 'Draw'])]
        
        if not results_agg.empty:
            fig_results = px.bar(
                results_agg,
                x='result_label',
                y='percentage',
                color='game_type',
                barmode='group',
                text='percentage',
                labels={'percentage': 'Percentage (%)', 'result_label': 'Result', 'game_type': 'Game Type'},
                color_discrete_map=COLORS
            )
            fig_results.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            fig_results.update_layout(yaxis_range=[0, max(results_agg['percentage']) * 1.15])
            st.plotly_chart(fig_results, use_container_width=True)
        else:
            st.info("No result data available yet")
    else:
        st.info("Result data not available - will appear as more games are processed")

    st.divider()

    # =========================================================================
    # SECTION 4: FULL GAME PHASE BREAKDOWN (NEW)
    # =========================================================================
    st.header("4. Game Phase Breakdown")
    st.caption("What phase do games end in? (Opening <15 moves, Middlegame 15-40, Endgame >40)")
    
    phases = data['phases'].copy()
    
    # Filter to only known phases
    phases = phases[phases['game_phase'].isin(['Opening', 'Middlegame', 'Endgame'])]
    
    if not phases.empty:
        total_per_type = phases.groupby('game_type')['count'].transform('sum')
        phases['percentage'] = (phases['count'] / total_per_type * 100).round(1)
        
        # Order phases logically
        phase_order = {'Opening': 0, 'Middlegame': 1, 'Endgame': 2}
        phases['phase_order'] = phases['game_phase'].map(phase_order)
        phases = phases.sort_values(['game_type', 'phase_order'])
        
        fig_phases = px.bar(
            phases,
            x='game_type',
            y='percentage',
            color='game_phase',
            text='percentage',
            labels={'percentage': 'Percentage (%)', 'game_type': 'Game Type', 'game_phase': 'Phase'},
            color_discrete_map={'Opening': '#FFA15A', 'Middlegame': '#FECB52', 'Endgame': '#636EFA'},
            category_orders={'game_phase': ['Opening', 'Middlegame', 'Endgame']}
        )
        fig_phases.update_traces(texttemplate='%{text:.1f}%', textposition='inside')
        fig_phases.update_layout(barmode='stack', yaxis_range=[0, 100])
        st.plotly_chart(fig_phases, use_container_width=True)
    else:
        st.info("Phase data not available yet")

    st.divider()

    # =========================================================================
    # SECTION 5: AVERAGE GAME LENGTH BY OPENING FAMILY (NEW)
    # =========================================================================
    st.header("5. Average Game Length by Opening Family")
    st.caption("Which openings lead to longer games?")
    
    avg_fam = data['avg_by_family'].copy()
    avg_fam = avg_fam[avg_fam['family_name'] != 'Unknown']  # Filter out unknown
    
    if not avg_fam.empty:
        fig_avg_fam = px.bar(
            avg_fam,
            x='family_name',
            y='avg_moves',
            color='game_type',
            barmode='group',
            text='avg_moves',
            labels={'avg_moves': 'Average Moves', 'family_name': 'Opening Family', 'game_type': 'Game Type'},
            color_discrete_map=COLORS
        )
        fig_avg_fam.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig_avg_fam.update_layout(yaxis_range=[0, max(avg_fam['avg_moves']) * 1.15])
        st.plotly_chart(fig_avg_fam, use_container_width=True)
    else:
        st.info("Opening family data not available yet")

    st.divider()

    # =========================================================================
    # SECTION 6: ECO FAMILY DISTRIBUTION (Pie Charts)
    # =========================================================================
    st.header("6. Opening Families Distribution (ECO Categories)")
    st.caption("A: Flank | B: Semi-Open | C: Open | D: Closed | E: Indian")

    col1, col2 = st.columns(2)
    
    fam_stats = data['openings'].groupby(['game_type', 'family_name'])['count'].sum().reset_index()
    
    with col1:
        st.subheader("Casual Games")
        casual_fam = fam_stats[fam_stats['game_type'] == 'casual']
        if not casual_fam.empty:
            fig_pie1 = px.pie(casual_fam, values='count', names='family_name', hole=0.4,
                            color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig_pie1, use_container_width=True)
        else:
            st.info("No casual data")

    with col2:
        st.subheader("Professional Games")
        pro_fam = fam_stats[fam_stats['game_type'] == 'professional']
        if not pro_fam.empty:
            fig_pie2 = px.pie(pro_fam, values='count', names='family_name', hole=0.4,
                            color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig_pie2, use_container_width=True)
        else:
            st.info("No professional data")

    st.divider()

    # =========================================================================
    # SECTION 7: TOP 10 SPECIFIC OPENINGS
    # =========================================================================
    st.header("7. Top 10 Specific Openings (ECO Codes)")
    
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
            fig_bar1.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_bar1, use_container_width=True)
        else:
            st.info("No casual data")
    
    with p_op:
        st.subheader("Professional: Top ECOs")
        top_pro = ops[ops['game_type'] == 'professional'].nlargest(10, 'count')
        if not top_pro.empty:
            fig_bar2 = px.bar(
                top_pro, x='count', y='eco', orientation='h',
                color='count', color_continuous_scale='Magma'
            )
            fig_bar2.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_bar2, use_container_width=True)
        else:
            st.info("No professional data")
            
    # Auto-refresh only if streaming is not complete
    if not check_streaming_complete():
        time.sleep(2)  # Faster refresh for live feel
        rerun()
    else:
        st.balloons()  # Celebrate completion!

if __name__ == "__main__":
    main()
