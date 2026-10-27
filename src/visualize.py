import sqlite3
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

conn = sqlite3.connect("data/market_data.db")

query = """
SELECT timestamp, open, high, low, close, volume, price_spread, return_pct
FROM crypto_candles
WHERE symbol = 'BTC'
ORDER BY timestamp ASC;
"""
df = pd.read_sql_query(query, conn)
conn.close()

if df.empty:
    print("No records found in market_data.db yet. Trigger your Airflow DAG first!")
    exit()

fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.08,
    row_heights=[0.7, 0.3],
    subplot_titles=("Test", "Trading Volume")
)

fig.add_trace(
    go.Candlestick(
        x=df["timestamp"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name="OHLC"
    ),
    row=1, col=1
)

fig.add_trace(
    go.Bar(
        x=df["timestamp"],
        y=df["volume"],
        name="Volume",
        marker_color="rgba(100, 150, 250, 0.6)"
    ),
    row=2, col=1
)

fig.update_layout(
    title="Market Overview",
    xaxis_rangeslider_visible=False,
    template="plotly_dark",
    height=750
)

fig.show()