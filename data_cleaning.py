import pandas as pd
import numpy as np

# Step 1: Load CSV
df = pd.read_csv("stock_market.csv")

# Step 2: Normalize column names (snake_case)
df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]

# Step 3: Trim whitespace from string cells
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Step 4: Standardize missing values
missing_tokens = {"", "na", "n/a", "null", "-", "none"}
df = df.applymap(lambda x: np.nan if isinstance(x, str) and x.lower() in missing_tokens else x)

# Step 5: Convert trade_date to datetime (yyyy-MM-dd)
df['trade_date'] = pd.to_datetime(df['trade_date'], dayfirst=True, errors='coerce')

# Step 6: Optional — uppercase tickers
if 'ticker' in df.columns:
    df['ticker'] = df['ticker'].str.upper()

# Step 7: Show cleaned preview
print("Cleaned data preview:")
print(df.head())

print("\nNull summary after cleaning:")
print(df.isna().sum())
# --- Step 8: Convert types ---

# Numeric columns
for col in ['open_price', 'close_price']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

if 'volume' in df.columns:
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')

# Boolean column
def to_bool(x):
    if pd.isna(x):
        return pd.NA
    x = str(x).strip().lower()
    if x in ['yes', 'y', 'true', 't', '1']:
        return True
    elif x in ['no', 'n', 'false', 'f', '0']:
        return False
    else:
        return pd.NA

if 'validated' in df.columns:
    df['validated'] = df['validated'].apply(to_bool).astype('boolean')

# --- Step 9: Deduplicate rows ---
before = len(df)
df = df.drop_duplicates()
after = len(df)
print(f"Deduplicated rows: before={before}, after={after}")

# --- Step 10: Save cleaned parquet ---
df.to_parquet("cleaned.parquet", index=False)
print("Saved cleaned.parquet")

# --- Step 11: Aggregations ---

# 1) Daily average close by ticker
agg1 = df.dropna(subset=['trade_date', 'ticker', 'close_price'])
agg1 = agg1.groupby(['trade_date','ticker'], as_index=False).agg(
    avg_close=('close_price','mean'),
    count_obs=('close_price','count')
)
agg1.to_parquet("agg1.parquet", index=False)
print("Saved agg1.parquet (daily avg close by ticker)")

# 2) Average volume by sector
agg2 = df.dropna(subset=['sector','volume'])
agg2 = agg2.groupby('sector', as_index=False).agg(
    avg_volume=('volume','mean'),
    total_volume=('volume','sum'),
    observations=('volume','count')
)
agg2.to_parquet("agg2.parquet", index=False)
print("Saved agg2.parquet (avg volume by sector)")

# 3) Simple daily return by ticker
agg3_list = []
for ticker, g in df.dropna(subset=['ticker','trade_date','close_price']).groupby('ticker'):
    g = g.sort_values('trade_date').copy()
    g['prev_close'] = g['close_price'].shift(1)
    g['daily_return'] = (g['close_price'] / g['prev_close']) - 1
    agg3_list.append(g[['trade_date','ticker','close_price','prev_close','daily_return']])
agg3 = pd.concat(agg3_list, ignore_index=True)
agg3.to_parquet("agg3.parquet", index=False)
print("Saved agg3.parquet (daily return by ticker)")
