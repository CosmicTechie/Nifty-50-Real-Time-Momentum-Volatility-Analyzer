import pandas as pd
import numpy as np
import ta # Technical Analysis Library

def analyze_technical_indicators(df):
    """
    Calculates Technical Indicators (RSI, Volatility) for all stocks in the dataframe.
    Returns a dictionary with the latest stats for each stock.
    """
    results = {}
    unique_tickers = df['product_id'].unique()
    
    for ticker in unique_tickers:
        # Filter data for this specific stock
        stock_df = df[df['product_id'] == ticker].copy().sort_values(by='date')
        
        # Need at least 15 days of data to calculate 14-day RSI
        if len(stock_df) < 15:
            continue 
            
        # 1. Calculate RSI (Momentum)
        stock_df['rsi'] = ta.momentum.rsi(stock_df['avg_price'], window=14)
        
        # 2. Calculate Annualized Volatility (Risk)
        stock_df['pct_change'] = stock_df['avg_price'].pct_change()
        volatility = stock_df['pct_change'].std() * np.sqrt(252) # 252 trading days/year
        
        # Get the latest values (The "Live" Signal)
        current_rsi = stock_df['rsi'].iloc[-1]
        
        # 3. Generate Signal Logic
        signal = "HOLD / NEUTRAL"
        signal_color = "gray"
        
        if current_rsi > 70:
            signal = "SELL (Overbought)"
            signal_color = "red"
        elif current_rsi < 30:
            signal = "BUY (Oversold)"
            signal_color = "green"
        elif current_rsi > 60:
            signal = "Watch (Trending High)"
            signal_color = "orange"
        elif current_rsi < 40:
            signal = "Watch (Trending Low)"
            signal_color = "blue"
            
        results[ticker] = {
            "rsi": current_rsi,
            "volatility": volatility,
            "signal": signal,
            "signal_color": signal_color
        }
        
    return results