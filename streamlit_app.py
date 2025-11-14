import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

st.title("Stock Market Dashboard")

# --- Load data ---
df_cleaned = pd.read_parquet("cleaned.parquet")
agg1 = pd.read_parquet("agg1.parquet")
agg2 = pd.read_parquet("agg2.parquet")
agg3 = pd.read_parquet("agg3.parquet")

# --- Filters ---
st.sidebar.header("Filters")

# Date range filter
min_date = df_cleaned['trade_date'].min()
max_date = df_cleaned['trade_date'].max()
date_range = st.sidebar.date_input("Select date range", [min_date, max_date])

# Ticker filter
tickers = df_cleaned['ticker'].dropna().unique()
selected_ticker = st.sidebar.selectbox("Select ticker", ["All"] + list(tickers))

# Sector filter
sectors = df_cleaned['sector'].dropna().unique()
selected_sector = st.sidebar.selectbox("Select sector", ["All"] + list(sectors))

# --- Filtered data ---
filtered_df = df_cleaned.copy()
if selected_ticker != "All":
    filtered_df = filtered_df[filtered_df['ticker']==selected_ticker]
if selected_sector != "All":
    filtered_df = filtered_df[filtered_df['sector']==selected_sector]
filtered_df = filtered_df[
    (filtered_df['trade_date'] >= pd.to_datetime(date_range[0])) &
    (filtered_df['trade_date'] <= pd.to_datetime(date_range[1]))
]

st.subheader("Filtered Data Preview")
st.dataframe(filtered_df)

# --- Plot 1: Daily Average Close (agg1) ---
agg1_plot = agg1.copy()
if selected_ticker != "All":
    agg1_plot = agg1_plot[agg1_plot['ticker']==selected_ticker]

fig1 = px.line(agg1_plot, x='trade_date', y='avg_close', color='ticker', title="Daily Average Close Price")
st.plotly_chart(fig1, use_container_width=True)

# --- Plot 2: Avg Volume by Sector (agg2) ---
agg2_plot = agg2.copy()
if selected_sector != "All":
    agg2_plot = agg2_plot[agg2_plot['sector']==selected_sector]

fig2 = px.bar(agg2_plot, x='sector', y='avg_volume', title="Average Volume by Sector")
st.plotly_chart(fig2, use_container_width=True)

# --- Plot 3: Daily Return by Ticker (agg3) ---
agg3_plot = agg3.copy()
if selected_ticker != "All":
    agg3_plot = agg3_plot[agg3_plot['ticker']==selected_ticker]

fig3 = px.line(agg3_plot, x='trade_date', y='daily_return', color='ticker', title="Daily Return by Ticker")
st.plotly_chart(fig3, use_container_width=True)
