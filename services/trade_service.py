from services.stock_service import get_stock_price

def buy_stock(db, notifier, user_id, symbol, quantity):
    user = db.get_user(user_id)
    price = get_stock_price(symbol)
    cost = price * quantity

    if user["balance"] < cost:
        return False, "Insufficient balance"

    user["balance"] -= cost
    user["portfolio"][symbol] = user["portfolio"].get(symbol, 0) + quantity

    notifier.send(f"Bought {quantity} {symbol} @ {price:.2f}")
    return True, "Trade successful"
