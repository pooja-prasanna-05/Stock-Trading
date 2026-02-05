# app.py
"""
Main Flask application for Virtual Stock Trading Platform.
"""

import os
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    jsonify,
    flash,
)

from aws_client import AwsClient

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret-key-change-in-production")

aws_client = AwsClient()

# Admin password (change this!)
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")


# ---------- Auth helpers ----------

def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapper


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            return redirect(url_for("admin_login"))
        return view_func(*args, **kwargs)
    return wrapper


def get_current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return aws_client.get_user_by_id(user_id)


# ---------- Routes: Main Pages ----------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "").strip()
        
        if not email or not password:
            flash("Email and password are required.", "error")
            return redirect(url_for("signup"))

        try:
            user = aws_client.create_user(email=email, password=password)
            session["user_id"] = user.user_id
            flash("Signup successful! Welcome!", "success")
            return redirect(url_for("dashboard"))
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("signup"))

    return render_template("signup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "").strip()
        
        user = aws_client.get_user_by_email(email)
        if not user or user.password != password:
            flash("Invalid email or password.", "error")
            return redirect(url_for("login"))

        session["user_id"] = user.user_id
        flash("Logged in successfully.", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("index"))


@app.route("/dashboard")
@login_required
def dashboard():
    user = get_current_user()
    if not user:
        return redirect(url_for("login"))
    
    holdings = aws_client.get_portfolio(user.user_id)
    trades = aws_client.get_trades(user.user_id)
    
    return render_template(
        "dashboard.html",
        user=user,
        holdings=holdings,
        trades=trades,
    )


# ---------- API Routes: Stocks & Trading ----------

@app.route("/api/stocks")
@login_required
def api_stocks():
    q = request.args.get("q", "").strip()
    stocks = aws_client.get_all_stocks(q)
    return jsonify(stocks)


@app.route("/api/stocks/<symbol>")
@login_required
def api_stock(symbol):
    stock = aws_client.get_stock(symbol)
    if not stock:
        return jsonify({"error": "Stock not found"}), 404
    return jsonify(stock)


@app.route("/api/portfolio")
@login_required
def api_portfolio():
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    
    holdings = aws_client.get_portfolio(user.user_id)
    stocks_map = {s["symbol"]: s for s in aws_client.get_all_stocks("")}

    result = []
    for h in holdings:
        stock = stocks_map.get(h.symbol)
        current_price = stock["price"] if stock else 0.0
        market_value = h.quantity * current_price
        cost = h.quantity * h.avg_buy_price
        unrealized_pl = market_value - cost
        result.append(
            {
                "symbol": h.symbol,
                "quantity": h.quantity,
                "avg_buy_price": h.avg_buy_price,
                "current_price": round(current_price, 2),
                "market_value": round(market_value, 2),
                "unrealized_pl": round(unrealized_pl, 2),
            }
        )

    return jsonify(
        {
            "cash_balance": user.cash_balance,
            "holdings": result,
        }
    )


@app.route("/api/trades")
@login_required
def api_trades():
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    
    trades = aws_client.get_trades(user.user_id)
    return jsonify([t.__dict__ for t in trades])


@app.route("/api/orders", methods=["POST"])
@login_required
def api_orders():
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    
    data = request.get_json() or {}
    symbol = (data.get("symbol") or "").upper().strip()
    side = (data.get("side") or "").upper().strip()
    
    try:
        quantity = int(data.get("quantity", 0))
    except (TypeError, ValueError):
        quantity = 0

    if not symbol or side not in ("BUY", "SELL") or quantity <= 0:
        return jsonify({"error": "symbol, side (BUY/SELL), quantity>0 required"}), 400

    stock = aws_client.get_stock(symbol)
    if not stock:
        return jsonify({"error": "Invalid symbol"}), 400

    try:
        trade = aws_client.execute_trade(
            user=user, symbol=symbol, side=side, qty=quantity, price=stock["price"]
        )
        # Refresh user after trade
        user = aws_client.get_user_by_id(user.user_id)
        
        return jsonify(
            {
                "message": "Trade executed successfully",
                "trade": trade.__dict__,
                "cash_balance": user.cash_balance,
            }
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


# ---------- Admin Routes ----------

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == ADMIN_PASSWORD:
            session["is_admin"] = True
            flash("Admin logged in successfully.", "success")
            return redirect(url_for("admin_dashboard"))
        flash("Invalid admin password.", "error")
        return redirect(url_for("admin_login"))
    
    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    flash("Admin logged out.", "success")
    return redirect(url_for("index"))


@app.route("/admin/dashboard")
@admin_required
def admin_dashboard():
    users = aws_client.admin_get_all_users()
    trades = aws_client.admin_get_all_trades()
    return render_template("admin_dashboard.html", users=users, trades=trades)


if __name__ == "__main__":
    # Production mode: disable debug, listen on all interfaces
    # On EC2, ensure security group allows port 5000 (or use port 80 with nginx)
    debug_mode = os.getenv("FLASK_ENV", "production") == "development"
    port = int(os.getenv("PORT", 5000))
    app.run(debug=debug_mode, host="0.0.0.0", port=port)
