from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

df = pd.read_csv("data.csv")
df.columns = [c.lower() for c in df.columns]
df["time"] = (df["timestamp"] // 1000).astype(int)
df = df[["time", "open", "high", "low", "close"]].sort_values("time")

def resample(data, sec):
    data["b"] = data["time"] // sec * sec
    g = data.groupby("b")
    return pd.DataFrame({
        "time": g.first()["b"],
        "open": g.first()["open"],
        "high": g.max()["high"],
        "low": g.min()["low"],
        "close": g.last()["close"],
    })

@app.get("/candles")
def candles(start: int | None = None, end: int | None = None, tf="1m"):
    data = df.copy()
    if start: data = data[data["time"] >= start]
    if end: data = data[data["time"] <= end]

    tf_map = {"1m":60,"5m":300,"15m":900,"1h":3600,"4h":14400}
    if tf != "1m":
        data = resample(data, tf_map[tf])

    return data.to_dict("records")
