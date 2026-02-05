import requests
from config import STOCK_API_KEY, STOCK_API_URL

def get_stock_price(symbol):
    if not STOCK_API_KEY:
        raise RuntimeError("Missing STOCK_API_KEY")

    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": STOCK_API_KEY
    }

    response = requests.get(STOCK_API_URL, params=params, timeout=5)
    response.raise_for_status()

    data = response.json()

    try:
        price = float(data["Global Quote"]["05. price"])
        return price
    except (KeyError, ValueError):
        raise RuntimeError("Invalid API response")
