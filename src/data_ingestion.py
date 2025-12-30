import yfinance as yf
import pandas as pd
import os
import datetime
import time
from tqdm import tqdm  # Progress bar

# --- CONFIGURATION ---
# We save to the parent directory's data folder
OUTPUT_PATH = '../data/raw/nifty50_history.csv'

# FULL NIFTY 50 LIST (Updated for NSE Suffix .NS)
NIFTY_50_TICKERS = {
    'RELIANCE.NS': 'Reliance Industries',
    'TCS.NS': 'Tata Consultancy Services',
    'HDFCBANK.NS': 'HDFC Bank',
    'ICICIBANK.NS': 'ICICI Bank',
    'INFY.NS': 'Infosys',
    'BHARTIARTL.NS': 'Bharti Airtel',
    'ITC.NS': 'ITC Ltd',
    'SBIN.NS': 'State Bank of India',
    'LICI.NS': 'LIC India',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'LT.NS': 'Larsen & Toubro',
    'BAJFINANCE.NS': 'Bajaj Finance',
    'MARUTI.NS': 'Maruti Suzuki',
    'HCLTECH.NS': 'HCL Technologies',
    'SUNPHARMA.NS': 'Sun Pharma',
    'ADANIENT.NS': 'Adani Enterprises',
    'TITAN.NS': 'Titan Company',
    'KOTAKBANK.NS': 'Kotak Mahindra Bank',
    'ONGC.NS': 'ONGC',
    'TMPV.NS': 'Tata Motors (Passenger & EV)',  
    'NTPC.NS': 'NTPC',
    'AXISBANK.NS': 'Axis Bank',
    'ADANIPORTS.NS': 'Adani Ports',
    'ULTRACEMCO.NS': 'UltraTech Cement',
    'M&M.NS': 'Mahindra & Mahindra',
    'WIPRO.NS': 'Wipro',
    'BAJAJFINSV.NS': 'Bajaj Finserv',
    'JSWSTEEL.NS': 'JSW Steel',
    'POWERGRID.NS': 'Power Grid Corp',
    'TATASTEEL.NS': 'Tata Steel',
    'COALINDIA.NS': 'Coal India',
    'SBILIFE.NS': 'SBI Life Insurance',
    'HDFCLIFE.NS': 'HDFC Life Insurance',
    'GRASIM.NS': 'Grasim Industries',
    'TECHM.NS': 'Tech Mahindra',
    'BRITANNIA.NS': 'Britannia Industries',
    'HINDALCO.NS': 'Hindalco Industries',
    'EICHERMOT.NS': 'Eicher Motors',
    'INDUSINDBK.NS': 'IndusInd Bank',
    'DRREDDY.NS': 'Dr. Reddys Labs',
    'CIPLA.NS': 'Cipla',
    'TATACONSUM.NS': 'Tata Consumer Products',
    'DIVISLAB.NS': 'Divis Laboratories',
    'APOLLOHOSP.NS': 'Apollo Hospitals',
    'BAJAJ-AUTO.NS': 'Bajaj Auto',
    'HEROMOTOCO.NS': 'Hero MotoCorp',
    'ASIANPAINT.NS': 'Asian Paints',
    'UPL.NS': 'UPL Ltd',
    'BPCL.NS': 'BPCL',
    'SHREECEM.NS': 'Shree Cements'
}

def ingest_nifty50_data():
    print(f"🚀 [ {datetime.datetime.now().strftime('%H:%M:%S')} ] Starting NIFTY 50 Ingestion...")
    print(f"   -> Target: {len(NIFTY_50_TICKERS)} Companies")
    
    all_data = []
    failed_tickers = []
    
    # Using tqdm for progress bar
    for symbol, name in tqdm(NIFTY_50_TICKERS.items(), desc="Fetching Stocks"):
        
        # --- RATE LIMIT PROTECTION ---
        # Sleep for 0.5 seconds to be polite to Yahoo's free API
        time.sleep(0.5) 
        
        try:
            # Download 2 years of history
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="2y", interval="1d", auto_adjust=True)
            
            if df.empty:
                failed_tickers.append(symbol)
                continue
                
            df = df.reset_index()
            
            # Metadata & Cleaning
            df['product_id'] = symbol
            df['product_name'] = name
            
            df = df.rename(columns={
                'Date': 'date',
                'Close': 'avg_price',
                'Volume': 'qty_sold' 
            })
            
            # Remove timezone to avoid Pandas warnings
            df['date'] = df['date'].dt.tz_localize(None)
            
            # Keep file size small
            df = df[['date', 'product_id', 'product_name', 'avg_price', 'qty_sold']]
            all_data.append(df)
            
        except Exception as e:
            failed_tickers.append(f"{symbol} ({str(e)})")
        
    # Combine & Save
    if all_data:
        final_df = pd.concat(all_data).sort_values(by=['product_id', 'date'])
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        final_df.to_csv(OUTPUT_PATH, index=False)
        
        print(f"\n✅ Data Pipeline Complete!")
        print(f"   Saved to: {OUTPUT_PATH}")
        print(f"   Total Rows: {len(final_df):,}")
        
        if failed_tickers:
            print(f"   ⚠️ Warning: Could not fetch data for: {failed_tickers}")
    else:
        print("\n❌ Critical Failure: No data fetched. Check Internet Connection.")

if __name__ == "__main__":
    ingest_nifty50_data()