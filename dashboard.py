# dashboard_v6.py (full code with precise duration display & verification log)
# Requirements: pip install streamlit pandas websocket-client

import streamlit as st
import pandas as pd
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict 
import threading

# --- Attempt to import the detector module ---
try:
    import detector
    if hasattr(detector, 'start_threads') and callable(detector.start_threads):
        if 'detector_started_flag' not in st.session_state:
            print("Starting detector threads...")
            detector.start_threads()
            st.session_state['detector_started_flag'] = True
            print("Detector threads started.")
    else:
        print("Detector module loaded, but no start_threads function found or needed.")
except ImportError:
    st.error("Fatal Error: detector.py not found. Please ensure it's in the same directory.")
    st.stop()
except Exception as e:
    st.error(f"Fatal Error importing or starting detector: {e}")
    st.stop()

# --- Page Configuration ---
st.set_page_config(
    page_title="Arbitrage Opportunity Dashboard V6", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Custom CSS --- #
custom_css = """
<style>
    /* Base Styling */
    body {
        background-color: #0D1117 !important;
        color: #C9D1D9 !important;
        font-family: 'Inter', sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue';
    }
    .main .block-container {
        padding: 1.5rem 2rem;
    }
    /* Headings */
    h1 { color: #58A6FF; font-weight: 600; font-size: 2.0em; margin-bottom: 1rem; }
    h2 { color: #C9D1D9; font-weight: 500; font-size: 1.5em; margin-bottom: 0.8rem; border-bottom: 1px solid #30363D; padding-bottom: 0.4em; }
    h3 { color: #8B949E; font-weight: 500; font-size: 1.1em; margin-bottom: 0.5rem; margin-top: 1.5rem; }
    /* Containers & Metrics */
    [data-testid="stMetric"] {
        background-color: #161B22;
        border: 1px solid #30363D;
        border-radius: 8px;
        padding: 1rem 1.2rem;
        margin-bottom: 1rem;
    }
    [data-testid="stMetricLabel"] { color: #8B949E; font-size: 0.85em; font-weight: 500; margin-bottom: 0.2rem; }
    [data-testid="stMetricValue"] { color: #C9D1D9; font-size: 1.6em; font-weight: 600; }
    [data-testid="stMetricDelta"] { font-size: 0.85em; font-weight: 500; }
    /* Interval Report Specific Metric */
    .interval-report-metric {
        background-color: #21262D;
        border: 1px solid #58A6FF;
    }
    /* Discrepancy Tile Styling */
    .discrepancy-tile {
        background-color: #161B22;
        border: 1px solid #30363D;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
        text-align: center;
        transition: border-color 0.2s ease, background-color 0.2s ease;
        cursor: pointer;
        position: relative;
        min-height: 130px; 
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    .discrepancy-tile:hover {
        border-color: #58A6FF;
        background-color: #21262D;
    }
    .tile-symbol { font-size: 1.1em; font-weight: 600; color: #C9D1D9; margin-bottom: 0.5rem; display: block; }
    .tile-discrepancy { font-size: 2.0em; font-weight: 700; font-family: 'Roboto Mono', monospace; display: block; padding: 5px 0; border-radius: 5px; min-height: 1.5em; }
    .tile-ath {
        font-size: 0.75em;
        color: #8B949E; 
        margin-top: 0.6rem; 
        font-family: 'Roboto Mono', monospace;
        display: block;
    }
    .positive { color: #2ECC71; }
    .negative { color: #E74C3C; }
    .zero { color: #8B949E; }
    /* Flashing Animations */
    @keyframes flash-green-bg { 0%, 100% { background-color: transparent; } 50% { background-color: rgba(46, 204, 113, 0.5); } }
    @keyframes flash-red-bg { 0%, 100% { background-color: transparent; } 50% { background-color: rgba(231, 76, 60, 0.5); } }
    .flash-green { animation: flash-green-bg 0.6s ease-out; }
    .flash-red { animation: flash-red-bg 0.6s ease-out; }
    /* Details Section */
    .details-section { background-color: #161B22; border: 1px solid #30363D; border-radius: 8px; padding: 1.5rem; margin-top: 1.5rem; }
    /* Table Styling */
    .styled-table table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    .styled-table th, .styled-table td { border-bottom: 1px solid #30363D; padding: 0.6rem 0.8rem; text-align: left; }
    .styled-table th { background-color: #21262D; color: #C9D1D9; font-weight: 600; }
    .styled-table td { vertical-align: middle; }
    .styled-table .numeric-cell { text-align: right; font-family: 'Roboto Mono', monospace; }
    .tile-button-container { position: absolute; top: 0; left: 0; width: 100%; height: 100%; opacity: 0; z-index: 1; }
    .tile-button-container button { width: 100%; height: 100%; padding: 0; border: none; background: none; cursor: pointer; }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# --- Session State Initialization ---
if 'selected_symbol' not in st.session_state: st.session_state['selected_symbol'] = None
if 'previous_discrepancies' not in st.session_state: st.session_state['previous_discrepancies'] = {}
if 'flash_state' not in st.session_state: st.session_state['flash_state'] = {}
if 'all_time_high_discrepancies' not in st.session_state:
    st.session_state['all_time_high_discrepancies'] = defaultdict(lambda: Decimal('-inf'))
if 'selected_opportunity_id_for_verification' not in st.session_state:
    st.session_state['selected_opportunity_id_for_verification'] = None

# --- Helper Functions ---
def safe_decimal(value, default=Decimal('NaN')):
    if value is None: return default
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else default
    except (InvalidOperation, ValueError, TypeError): return default

def format_price(price_val):
    dec_price = safe_decimal(price_val)
    if dec_price.is_nan(): return "N/A"
    return f"{dec_price:,.8f}".rstrip('0').rstrip('.') if '.' in f"{dec_price:,.8f}" else f"{dec_price:,}"


def format_percentage(pct_val):
    dec_pct = safe_decimal(pct_val)
    return f"{dec_pct:+,.4f}%" if not dec_pct.is_nan() else "N/A"

def format_timestamp_ms(ts):
    """ Formats datetime object or NaT to H:M:S.ms string or N/A """
    if pd.isna(ts) or ts is None:
        return "N/A"
    if isinstance(ts, datetime):
        return ts.strftime('%H:%M:%S.%f')[:-3] # Milliseconds
    return str(ts) # Fallback

# --- Data Fetching Functions ---
def get_live_data():
    try:
        if hasattr(detector, 'latest_market_data') and detector.latest_market_data:
            with detector.data_lock: return dict(detector.latest_market_data)
        else: return {}
    except Exception as e:
        print(f"Error accessing detector.latest_market_data: {e}")
        return {}

def get_opportunity_log_data():
    try:
        if hasattr(detector, 'get_opportunity_log') and callable(detector.get_opportunity_log):
            return detector.get_opportunity_log()
        else: return []
    except Exception as e:
        print(f"Error accessing opportunity log: {e}")
        return []

def get_interval_report_data():
    try:
        if hasattr(detector, 'get_interval_report') and callable(detector.get_interval_report):
            return detector.get_interval_report()
        else: return {"symbol": None, "value": None, "timestamp": None}
    except Exception as e:
        print(f"Error accessing interval report: {e}")
        return {"symbol": None, "value": None, "timestamp": None}

def get_historical_interval_reports_data(symbol):
    try:
        if hasattr(detector, 'get_historical_interval_reports') and callable(detector.get_historical_interval_reports):
            return detector.get_historical_interval_reports(symbol=symbol)
        else: return []
    except Exception as e:
        print(f"Error accessing historical interval reports for {symbol}: {e}")
        return []

def get_verification_log_for_opp(opportunity_id):
    try:
        if hasattr(detector, 'get_verification_snapshots_for_opportunity') and callable(detector.get_verification_snapshots_for_opportunity):
            return detector.get_verification_snapshots_for_opportunity(opportunity_id)
        else:
            st.warning("Verification log function not available in detector module.")
            return []
    except Exception as e:
        st.error(f"Error fetching verification log for {opportunity_id}: {e}")
        return []


# --- UI Placeholders ---
st.title("Arbitrage Opportunity Dashboard V6") 
metrics_placeholder = st.empty()
interval_report_placeholder = st.empty()
st.markdown("---") # Visual separator
st.markdown("## Live Discrepancy Grid")
grid_placeholder = st.empty()
st.markdown("---") # Visual separator
details_section_placeholder = st.empty()
verification_log_placeholder = st.empty() # For the new verification log table

# --- Main Update Logic ---
def update_dashboard():
    live_data = get_live_data()
    opportunity_log_full = get_opportunity_log_data() # Now contains 'opportunity_id'
    interval_report_summary = get_interval_report_data()
    selected_symbol = st.session_state.get('selected_symbol')
    selected_opp_id_for_verification = st.session_state.get('selected_opportunity_id_for_verification')

    # Update flash state for tiles
    new_flash_state = {}
    for symbol, data in live_data.items():
        current_disc = safe_decimal(data.get('discrepancy'))
        previous_disc = st.session_state['previous_discrepancies'].get(symbol, Decimal('NaN'))
        flash_class = ''
        if not current_disc.is_nan():
            current_ath = st.session_state['all_time_high_discrepancies'][symbol]
            if current_disc > current_ath: # Check against ATH
                st.session_state['all_time_high_discrepancies'][symbol] = current_disc
            if not previous_disc.is_nan(): # Compare with previous for flash
                if current_disc > previous_disc: flash_class = 'flash-green'
                elif current_disc < previous_disc: flash_class = 'flash-red'
        new_flash_state[symbol] = flash_class
        st.session_state['previous_discrepancies'][symbol] = current_disc
    st.session_state['flash_state'] = new_flash_state

    # --- Metrics Display ---
    with metrics_placeholder.container():
        metric_cols = st.columns(4)
        metric_cols[0].metric(label="Tracked Symbols", value=len(detector.SYMBOLS_TO_TRACK) if hasattr(detector, 'SYMBOLS_TO_TRACK') else "N/A")
        
        highest_live_disc = Decimal('-inf')
        highest_live_symbol = "N/A"
        if live_data:
            for sym, data_item in live_data.items():
                disc_val = safe_decimal(data_item.get('discrepancy'))
                if not disc_val.is_nan() and disc_val > highest_live_disc:
                    highest_live_disc = disc_val
                    highest_live_symbol = sym
        
        metric_cols[1].metric(label="Highest Live Discrepancy",
                              value=format_percentage(highest_live_disc if highest_live_disc > Decimal('-inf') else None),
                              delta=highest_live_symbol if highest_live_symbol != "N/A" else "")
        metric_cols[2].metric(label="Total Opportunities Logged", value=len(opportunity_log_full))
        metric_cols[3].metric(label="Last Update", value=datetime.now().strftime("%H:%M:%S"))

    # --- Interval Report Summary ---
    with interval_report_placeholder.container():
        st.markdown("## Overall Interval Report Summary")
        report_symbol = interval_report_summary.get("symbol")
        report_value = interval_report_summary.get("value")
        report_ts = interval_report_summary.get("timestamp")

        if report_symbol and report_value is not None and report_ts:
            report_value_dec = safe_decimal(report_value)
            ts_str = report_ts.strftime("%Y-%m-%d %H:%M:%S") if isinstance(report_ts, datetime) else "N/A"
            label = f"Highest Discrepancy ({getattr(detector, 'REPORTING_INTERVAL_MIN', 'N/A')} min interval, excluding active ops)"
            
            st.markdown(f"""
            <div data-testid="stMetric" class="interval-report-metric">
                <div data-testid="stMetricLabel">{label}</div>
                <div data-testid="stMetricValue">{format_percentage(report_value_dec)}</div>
                <div data-testid="stMetricDelta">{report_symbol} (as of {ts_str})</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info(f"Waiting for first {getattr(detector, 'REPORTING_INTERVAL_MIN', 'N/A')}-minute overall interval report...")

    # --- Live Discrepancy Grid ---
    with grid_placeholder.container():
        symbols_sorted = sorted(live_data.keys())
        if not symbols_sorted: st.info("Waiting for market data for grid...")
        else:
            cols_per_row = st.slider("Tiles per row", 3, 8, 5, key="grid_cols_slider") # User can adjust
            num_symbols = len(symbols_sorted)
            for r_idx in range((num_symbols + cols_per_row - 1) // cols_per_row):
                tile_cols = st.columns(cols_per_row)
                for c_idx_in_row in range(cols_per_row):
                    sym_idx = r_idx * cols_per_row + c_idx_in_row
                    if sym_idx < num_symbols:
                        symbol_key = symbols_sorted[sym_idx]
                        data_val = live_data.get(symbol_key, {})
                        current_disc_val = safe_decimal(data_val.get('discrepancy'))
                        flash_cl = st.session_state['flash_state'].get(symbol_key, '')
                        color_cl = "positive" if not current_disc_val.is_nan() and current_disc_val > 0 else \
                                   ("negative" if not current_disc_val.is_nan() and current_disc_val < 0 else "zero")
                        session_ath_val = st.session_state['all_time_high_discrepancies'][symbol_key]
                        ath_disp = format_percentage(session_ath_val if session_ath_val > Decimal('-inf') else None)
                        
                        tile_html_content = f"""
                        <div class='discrepancy-tile' id='tile-div-{symbol_key}'>
                            <span class='tile-symbol'>{symbol_key}</span>
                            <span class='tile-discrepancy {color_cl} {flash_cl}'>{format_percentage(current_disc_val)}</span>
                            <span class='tile-ath'>Session ATH: {ath_disp}</span>
                            <div class='tile-button-container'></div>
                        </div>"""
                        
                        tile_cols[c_idx_in_row].markdown(tile_html_content, unsafe_allow_html=True)
                        btn_key = f"btn_tile_{symbol_key}" # Unique key for each tile button
                        if tile_cols[c_idx_in_row].button(" ", key=btn_key, help=f"Click to view details for {symbol_key}"):
                            st.session_state['selected_symbol'] = symbol_key
                            st.session_state['selected_opportunity_id_for_verification'] = None # Clear verification selection
                            st.rerun() # Rerun to reflect selection immediately

    # --- Details Section for Selected Symbol ---
    with details_section_placeholder.container():
        if selected_symbol:
            st.markdown(f"<div class='details-section'>", unsafe_allow_html=True)
            st.markdown(f"## Details for {selected_symbol}")

            selected_data_live = live_data.get(selected_symbol, {})
            if selected_data_live:
                st.markdown("### Live Data Points for Verification")
                d_col1, d_col2, d_col3, d_col4 = st.columns(4)
                cb_live = selected_data_live.get('coinbase', {})
                bu_live = selected_data_live.get('bitunix', {})
                
                d_col1.metric("Coinbase Ask", format_price(cb_live.get('ask')), f"TS: {format_timestamp_ms(cb_live.get('timestamp'))}")
                d_col2.metric("Coinbase Bid", format_price(cb_live.get('bid')), f"TS: {format_timestamp_ms(cb_live.get('timestamp'))}")
                d_col3.metric("Bitunix Ask", format_price(bu_live.get('ask')), f"TS: {format_timestamp_ms(bu_live.get('timestamp'))}")
                d_col4.metric("Bitunix Bid", format_price(bu_live.get('bid')), f"TS: {format_timestamp_ms(bu_live.get('timestamp'))}")
                
                disc_cb_bu_live = selected_data_live.get('discrepancy_cb_bu')
                disc_bu_cb_live = selected_data_live.get('discrepancy_bu_cb')
                disc_live_col1, disc_live_col2 = st.columns(2)
                with disc_live_col1:
                    st.markdown(f"**Calc. Discrepancy (Buy Coinbase, Sell Bitunix):**")
                    st.markdown(f"<p style='font-family:\"Roboto Mono\", monospace; font-size: 1.1em;'>{format_percentage(disc_cb_bu_live)}</p>", unsafe_allow_html=True)
                with disc_live_col2:
                    st.markdown(f"**Calc. Discrepancy (Buy Bitunix, Sell Coinbase):**")
                    st.markdown(f"<p style='font-family:\"Roboto Mono\", monospace; font-size: 1.1em;'>{format_percentage(disc_bu_cb_live)}</p>", unsafe_allow_html=True)
                st.markdown(f"**Reported Max Live Discrepancy (from grid):**")
                st.markdown(f"<p style='font-family:\"Roboto Mono\", monospace; font-size: 1.1em;'>{format_percentage(selected_data_live.get('discrepancy'))}</p>", unsafe_allow_html=True)
                st.markdown("---")

            st.markdown("### All Arbitrage Opportunities Logged")
            symbol_opps_list = [opp for opp in opportunity_log_full if opp.get('symbol') == selected_symbol]
            if symbol_opps_list:
                df_opps_for_symbol = pd.DataFrame(symbol_opps_list)
                
                if 'opportunity_id' not in df_opps_for_symbol.columns: # Defensive check
                    df_opps_for_symbol['opportunity_id'] = [f"no_id_{i}" for i in range(len(df_opps_for_symbol))]

                # Ensure necessary columns exist before proceeding
                required_opp_cols = ['start', 'duration', 'peak_pct', 'direction', 'price1', 'price2', 'opportunity_id']
                if all(col in df_opps_for_symbol.columns for col in required_opp_cols):
                    df_opps_for_symbol['peak_pct'] = df_opps_for_symbol['peak_pct'].apply(safe_decimal)
                    df_opps_for_symbol['price1'] = df_opps_for_symbol['price1'].apply(safe_decimal)
                    df_opps_for_symbol['price2'] = df_opps_for_symbol['price2'].apply(safe_decimal)
                    df_opps_for_symbol['start'] = pd.to_datetime(df_opps_for_symbol['start'])
                    df_opps_for_symbol = df_opps_for_symbol.sort_values(by="start", ascending=False)

                    # Display opportunities with verification buttons
                    st.markdown("""
                    <div class='styled-table opportunity-table'>
                        <table>
                            <thead>
                                <tr><th>Start Time</th><th>Duration</th><th>Direction</th><th>Peak (%)</th><th>Price 1</th><th>Price 2</th><th>Opp. ID</th><th>Verify</th></tr>
                            </thead>
                            <tbody>
                    """, unsafe_allow_html=True)

                    for _, row_data in df_opps_for_symbol.iterrows():
                        opp_id_val = row_data['opportunity_id']
                        verify_button_key = f"verify_btn_{opp_id_val}"
                        
                        # Create table row HTML (Streamlit buttons don't easily go inside raw HTML tables)
                        # So, we use st.columns for each row for button functionality
                        row_cols = st.columns([3, 2, 1.5, 1.5, 1.5, 1.5, 3, 1]) # Adjust ratios as needed
                        row_cols[0].markdown(f"<div style='font-size:0.9em;'>{row_data['start'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}</div>", unsafe_allow_html=True)
                        row_cols[1].markdown(f"<div style='font-size:0.9em;'>{str(row_data['duration']) if pd.notna(row_data['duration']) else 'N/A'}</div>", unsafe_allow_html=True)
                        row_cols[2].markdown(f"<div style='font-size:0.9em;'>{row_data['direction']}</div>", unsafe_allow_html=True)
                        row_cols[3].markdown(f"<div style='font-size:0.9em; text-align:right;'>{format_percentage(row_data['peak_pct'])}</div>", unsafe_allow_html=True)
                        row_cols[4].markdown(f"<div style='font-size:0.9em; text-align:right;'>{format_price(row_data['price1'])}</div>", unsafe_allow_html=True)
                        row_cols[5].markdown(f"<div style='font-size:0.9em; text-align:right;'>{format_price(row_data['price2'])}</div>", unsafe_allow_html=True)
                        row_cols[6].markdown(f"<div style='font-size:0.8em; word-break:break-all;'>{opp_id_val}</div>", unsafe_allow_html=True)

                        if row_cols[7].button("🔍", key=verify_button_key, help=f"View verification log for {opp_id_val}"):
                            st.session_state['selected_opportunity_id_for_verification'] = opp_id_val
                            st.rerun() # Rerun to update the verification log display immediately
                    st.markdown("</tbody></table></div>", unsafe_allow_html=True)
                else:
                     st.warning(f"Opportunity log for {selected_symbol} is missing some required columns for display.")
            else:
                st.info(f"No arbitrage opportunities logged for {selected_symbol} yet.")
            st.markdown("---")


            st.markdown(f"### Closest Opportunities ({getattr(detector, 'REPORTING_INTERVAL_MIN', 'N/A')} min Intervals, No Active Ops)")
            historical_reports_list = get_historical_interval_reports_data(selected_symbol)
            if historical_reports_list:
                df_hist_reports_for_symbol = pd.DataFrame(historical_reports_list)
                if 'timestamp' in df_hist_reports_for_symbol.columns and \
                   'cb_to_bu' in df_hist_reports_for_symbol.columns and \
                   'bu_to_cb' in df_hist_reports_for_symbol.columns:
                    
                    df_hist_reports_for_symbol['cb_to_bu'] = df_hist_reports_for_symbol['cb_to_bu'].apply(safe_decimal)
                    df_hist_reports_for_symbol['bu_to_cb'] = df_hist_reports_for_symbol['bu_to_cb'].apply(safe_decimal)
                    df_hist_reports_for_symbol['timestamp'] = pd.to_datetime(df_hist_reports_for_symbol['timestamp'])
                    df_hist_reports_for_symbol = df_hist_reports_for_symbol.sort_values(by="timestamp", ascending=False)
                    
                    df_hist_display = df_hist_reports_for_symbol.copy()
                    df_hist_display['cb_to_bu'] = df_hist_reports_for_symbol['cb_to_bu'].apply(format_percentage)
                    df_hist_display['bu_to_cb'] = df_hist_reports_for_symbol['bu_to_cb'].apply(format_percentage)
                    df_hist_display['timestamp'] = df_hist_reports_for_symbol['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    df_hist_display = df_hist_display[['timestamp', 'cb_to_bu', 'bu_to_cb']]
                    df_hist_display.columns = ['Interval End Time', 'Closest CB->BU (%)', 'Closest BU->CB (%)']
                    
                    # Simpler table display using st.dataframe for this section
                    st.dataframe(df_hist_display, use_container_width=True, hide_index=True)
                else:
                    st.warning(f"Historical interval report data for {selected_symbol} is missing required columns.")
            else:
                st.info(f"No historical interval reports (closest opportunities) available for {selected_symbol} yet.")
            
            st.markdown(f"</div>", unsafe_allow_html=True) # End of details-section
    
    # --- Verification Log Display Section ---
    with verification_log_placeholder.container():
        if selected_opp_id_for_verification:
            st.markdown(f"<div class='details-section' style='margin-top: 20px;'>", unsafe_allow_html=True)
            st.markdown(f"### Verification Snapshot Log for Opportunity: `{selected_opp_id_for_verification}`")
            
            verification_data_list = get_verification_log_for_opp(selected_opp_id_for_verification)
            if verification_data_list:
                df_verify_log = pd.DataFrame(verification_data_list)

                # Format timestamps and numeric data for display
                if 'app_ts' in df_verify_log.columns:
                     df_verify_log['app_ts'] = pd.to_datetime(df_verify_log['app_ts']).dt.strftime('%H:%M:%S.%f')
                if 'coinbase_ts' in df_verify_log.columns:
                    df_verify_log['coinbase_ts'] = pd.to_datetime(df_verify_log['coinbase_ts'], errors='coerce').apply(format_timestamp_ms)
                if 'bitunix_ts' in df_verify_log.columns:
                    df_verify_log['bitunix_ts'] = pd.to_datetime(df_verify_log['bitunix_ts'], errors='coerce').apply(format_timestamp_ms)

                for col_name in ['coinbase_bid', 'coinbase_ask', 'bitunix_bid', 'bitunix_ask']:
                    if col_name in df_verify_log.columns: df_verify_log[col_name] = df_verify_log[col_name].apply(format_price)
                for col_name in ['disc_cb_bu', 'disc_bu_cb', 'max_disc']:
                     if col_name in df_verify_log.columns: df_verify_log[col_name] = df_verify_log[col_name].apply(format_percentage)
                
                # Define desired column order, handling missing columns gracefully
                desired_cols_verify = ['app_ts', 'coinbase_bid', 'coinbase_ask', 'coinbase_ts', 
                                       'bitunix_bid', 'bitunix_ask', 'bitunix_ts',
                                       'disc_cb_bu', 'disc_bu_cb', 'max_disc', 'source']
                
                # Filter df_verify_log to only include existing columns from desired_cols_verify
                existing_cols_in_df = [col for col in desired_cols_verify if col in df_verify_log.columns]
                df_verify_display = df_verify_log[existing_cols_in_df]

                st.dataframe(df_verify_display, height=600, use_container_width=True, hide_index=True)
            else:
                st.info(f"No verification snapshot data found or loaded for opportunity ID: {selected_opp_id_for_verification}")
            st.markdown(f"</div>", unsafe_allow_html=True)


# --- Initial Run & Auto-Refresh ---
update_dashboard()

# Add a refresh button to the sidebar for manual override
if st.sidebar.button("Force Refresh Dashboard", key="manual_refresh_sidebar"):
    st.rerun()

# The main auto-refresh loop - adjust sleep time as needed
# A short sleep allows for quick UI updates after interactions.
# For less frequent background updates, a longer sleep is fine if interactions trigger st.rerun().
REFRESH_INTERVAL_SECONDS = st.sidebar.slider("Dashboard Auto-Refresh (s)", 0.5, 5.0, 0.8, 0.1, key="refresh_slider")
time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()