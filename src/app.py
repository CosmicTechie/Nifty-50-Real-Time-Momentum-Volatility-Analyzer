import streamlit as st
import pandas as pd
import plotly.express as px
import ta
import os
import numpy as np # Explicit import for safety

# Import our own modules
from data_ingestion import ingest_nifty50_data 
from pricing_model import analyze_technical_indicators

# --- PAGE CONFIG ---
st.set_page_config(page_title="Nifty 50 Analyzer", layout="wide", page_icon="📈")
DATA_PATH = '../data/raw/nifty50_history.csv'

# --- HELPER FUNCTIONS ---
def load_data():
    """
    Robust Data Loader.
    1. Checks if CSV exists.
    2. If NOT, runs the ingestion script automatically (Self-Healing).
    3. Loads the CSV into Pandas.
    """
    if not os.path.exists(DATA_PATH):
        with st.spinner("⚠️ Data file not found. Downloading Nifty 50 data for the first time... (Approx 60s)"):
            ingest_nifty50_data()
            
    try:
        df = pd.read_csv(DATA_PATH)
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        return None

# --- MAIN DASHBOARD ---
st.title("🇮🇳 Nifty 50 Real-Time Analyzer")
st.markdown("### Professional Market Momentum Monitor")
st.markdown("Tracks **RSI** and **Volatility** for top Indian companies using live NSE data.")

# 1. SIDEBAR CONTROLS
st.sidebar.header("Control Panel")

# The "Manual Refresh" Button (Useful for Cloud Deployments)
if st.sidebar.button("🔄 Force Update Data"):
    with st.spinner("Connecting to NSE... Fetching latest prices..."):
        ingest_nifty50_data() # Re-runs the scraper
    st.success("Data Updated Successfully!")
    st.rerun() # Refreshes the web page

# 2. LOAD DATA
df = load_data()

if df is None:
    st.error("Critical Error: Could not load data. Please check your internet connection and try 'Force Update'.")
else:
    # 3. SEARCH & FILTER
    # Create a mapping for user-friendly names
    name_map = dict(zip(df['product_name'], df['product_id']))
    
    # Searchable Dropdown
    selected_name = st.sidebar.selectbox(
        "Search Company:", 
        options=sorted(list(name_map.keys()))
    )
    selected_ticker = name_map[selected_name]
    
    # Filter dataset for the selected stock
    stock_data = df[df['product_id'] == selected_ticker].sort_values(by='date')
    
    # 4. RUN ANALYSIS
    # Get latest signals from our logic model
    indicators = analyze_technical_indicators(df)
    stats = indicators.get(selected_ticker)
    
    # Calculate simple price change stats
    latest_price = stock_data['avg_price'].iloc[-1]
    prev_price = stock_data['avg_price'].iloc[-2]
    daily_change = latest_price - prev_price
    daily_pct = (daily_change / prev_price) * 100

    # 5. KPI DISPLAY
    st.divider()
    k1, k2, k3, k4 = st.columns(4)
    
    k1.metric("Current Price", f"₹{latest_price:,.2f}", f"{daily_pct:.2f}%")
    
    k2.metric("RSI (14-Day)", f"{stats['rsi']:.1f}", 
              help=">70 = Overbought (Sell Risk). <30 = Oversold (Buy Opportunity).")
    
    k3.metric("Annual Volatility", f"{stats['volatility']*100:.1f}%",
              help="Standard Deviation of returns (Risk Measure).")
    
    # Dynamic Signal Box
    signal_msg = stats['signal']
    if "BUY" in signal_msg:
        k4.success(f"Signal: {signal_msg}")
    elif "SELL" in signal_msg:
        k4.error(f"Signal: {signal_msg}")
    else:
        k4.info(f"Signal: {signal_msg}")

    # 6. INTERACTIVE CHARTS
    st.subheader(f"Technical Analysis: {selected_name}")
    
    tab1, tab2, tab3 = st.tabs(["Price Action", "RSI Momentum", "Raw Data"])
    
    with tab1:
        # Main Price Chart
        fig = px.line(stock_data, x='date', y='avg_price', title=f'{selected_name} - 2 Year Trend')
        fig.update_traces(line_color='#0068c9', line_width=2)
        st.plotly_chart(fig, use_container_width=True)
        
    with tab2:
        # RSI Chart
        # We calculate rolling RSI here just for plotting purposes
        stock_data['rsi_plot'] = ta.momentum.rsi(stock_data['avg_price'], window=14)
        
        fig_rsi = px.line(stock_data, x='date', y='rsi_plot', title='RSI Oscillator')
        
        # Add the Threshold Lines (70 and 30)
        fig_rsi.add_hline(y=70, line_dash="dot", line_color="red", annotation_text="Overbought (70)")
        fig_rsi.add_hline(y=30, line_dash="dot", line_color="green", annotation_text="Oversold (30)")
        
        # Fix Y-axis range to 0-100 standard
        fig_rsi.update_yaxes(range=[0, 100])
        
        st.plotly_chart(fig_rsi, use_container_width=True)

    with tab3:
        # Raw Data View
        st.dataframe(stock_data[['date', 'avg_price', 'qty_sold']].sort_values(by='date', ascending=False).head(100))