# aws_client.py
"""
AWS services integration for Virtual Stock Trading Platform.
Uses DynamoDB for data storage and SNS for trade notifications.
"""

import os
import uuid
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Key, Attr

USE_AWS = os.getenv("USE_AWS", "false").lower() == "true"


@dataclass
class User:
    user_id: str
    email: str
    password: str  # NOTE: In production, hash passwords
    cash_balance: float


@dataclass
class Holding:
    symbol: str
    quantity: int
    avg_buy_price: float


@dataclass
class Trade:
    trade_id: str
    user_id: str
    symbol: str
    side: str
    quantity: int
    price: float
    amount: float
    timestamp: str


class AwsClient:
    def __init__(self) -> None:
        self.use_aws = USE_AWS

        # In-memory fallback for local dev
        self._users: Dict[str, User] = {}
        self._users_by_email: Dict[str, str] = {}
        self._portfolio: Dict[str, Dict[str, Holding]] = {}
        self._trades: Dict[str, List[Trade]] = {}

        # Simulated stock universe
        self._stocks: Dict[str, Dict] = {
            "AAPL": {"symbol": "AAPL", "companyName": "Apple Inc.", "price": 180.0},
            "GOOG": {"symbol": "GOOG", "companyName": "Alphabet Inc.", "price": 135.0},
            "AMZN": {"symbol": "AMZN", "companyName": "Amazon.com Inc.", "price": 155.0},
            "TSLA": {"symbol": "TSLA", "companyName": "Tesla Inc.", "price": 220.0},
            "MSFT": {"symbol": "MSFT", "companyName": "Microsoft Corp.", "price": 320.0},
            "META": {"symbol": "META", "companyName": "Meta Platforms Inc.", "price": 350.0},
            "NVDA": {"symbol": "NVDA", "companyName": "NVIDIA Corp.", "price": 450.0},
            "NFLX": {"symbol": "NFLX", "companyName": "Netflix Inc.", "price": 420.0},
        }

        if self.use_aws:
            self._init_aws_clients()

    def _init_aws_clients(self) -> None:
        """Initialize AWS clients for DynamoDB and SNS."""
        region = os.getenv("AWS_REGION", "us-east-1")
        
        # DynamoDB resource
        self._dynamodb = boto3.resource("dynamodb", region_name=region)
        
        # SNS client
        self._sns = boto3.client("sns", region_name=region)
        
        # Table references
        self._users_table = self._dynamodb.Table(
            os.getenv("DYNAMODB_USERS_TABLE", "virtual_trading_users")
        )
        self._portfolio_table = self._dynamodb.Table(
            os.getenv("DYNAMODB_PORTFOLIO_TABLE", "virtual_trading_portfolio")
        )
        self._trades_table = self._dynamodb.Table(
            os.getenv("DYNAMODB_TRADES_TABLE", "virtual_trading_trades")
        )
        
        # SNS topic ARN
        self._trade_topic_arn = os.getenv("SNS_TRADE_TOPIC_ARN", "arn:aws:sns:us-east-1:699475957834:Stock_Trading")

    # ---------- Users ----------

    def create_user(self, email: str, password: str, initial_balance: float = 100000.0) -> User:
        """Create a new user."""
        email = email.lower().strip()
        
        if self.use_aws:
            # Check if user exists
            try:
                resp = self._users_table.get_item(Key={"email": email})
                if "Item" in resp:
                    raise ValueError("User already exists")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code not in ("ResourceNotFoundException", "ValidationException"):
                    raise

            user_id = str(uuid.uuid4())
            item = {
                "email": email,
                "user_id": user_id,
                "password": password,  # In production: hash this
                "cash_balance": initial_balance,
            }
            
            self._users_table.put_item(Item=item)
            return User(**item)
        
        # In-memory fallback
        if email in self._users_by_email:
            raise ValueError("User already exists")
        
        user_id = str(uuid.uuid4())
        user = User(
            user_id=user_id,
            email=email,
            password=password,
            cash_balance=initial_balance,
        )
        self._users[user_id] = user
        self._users_by_email[email] = user_id
        return user

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        email = email.lower().strip()
        
        if self.use_aws:
            try:
                resp = self._users_table.get_item(Key={"email": email})
                if "Item" not in resp:
                    return None
                item = resp["Item"]
                return User(
                    user_id=item["user_id"],
                    email=item["email"],
                    password=item["password"],
                    cash_balance=float(item["cash_balance"]),
                )
            except ClientError:
                return None
        
        # In-memory fallback
        user_id = self._users_by_email.get(email)
        if not user_id:
            return None
        return self._users[user_id]

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by user_id."""
        if self.use_aws:
            try:
                # Scan for user_id (for demo; in production, use GSI)
                resp = self._users_table.scan(
                    FilterExpression=Attr("user_id").eq(user_id)
                )
                items = resp.get("Items", [])
                if not items:
                    return None
                item = items[0]
                return User(
                    user_id=item["user_id"],
                    email=item["email"],
                    password=item["password"],
                    cash_balance=float(item["cash_balance"]),
                )
            except ClientError:
                return None
        
        # In-memory fallback
        return self._users.get(user_id)

    def update_user(self, user: User) -> None:
        """Update user (mainly cash balance)."""
        if self.use_aws:
            try:
                self._users_table.update_item(
                    Key={"email": user.email},
                    UpdateExpression="SET cash_balance = :cb",
                    ExpressionAttributeValues={":cb": user.cash_balance},
                )
            except ClientError:
                pass  # Fallback to in-memory
        
        # Always update in-memory cache
        self._users[user.user_id] = user
        self._users_by_email[user.email] = user.user_id

    # ---------- Portfolio ----------

    def _get_user_portfolio_map(self, user_id: str) -> Dict[str, Holding]:
        """Get portfolio map for user (creates if doesn't exist)."""
        if user_id not in self._portfolio:
            self._portfolio[user_id] = {}
        return self._portfolio[user_id]

    def get_portfolio(self, user_id: str) -> List[Holding]:
        """Get all holdings for a user."""
        if self.use_aws:
            try:
                resp = self._portfolio_table.query(
                    KeyConditionExpression=Key("user_id").eq(user_id)
                )
                holdings = []
                for item in resp.get("Items", []):
                    holdings.append(
                        Holding(
                            symbol=item["symbol"],
                            quantity=int(item["quantity"]),
                            avg_buy_price=float(item["avg_buy_price"]),
                        )
                    )
                return holdings
            except ClientError:
                pass
        
        # In-memory fallback
        return list(self._get_user_portfolio_map(user_id).values())

    def _update_portfolio(self, user_id: str, symbol: str, holding: Optional[Holding]) -> None:
        """Update portfolio entry."""
        if self.use_aws:
            try:
                if holding is None:
                    # Delete entry
                    self._portfolio_table.delete_item(
                        Key={"user_id": user_id, "symbol": symbol}
                    )
                else:
                    # Put/update entry
                    self._portfolio_table.put_item(
                        Item={
                            "user_id": user_id,
                            "symbol": symbol,
                            "quantity": holding.quantity,
                            "avg_buy_price": holding.avg_buy_price,
                        }
                    )
            except ClientError:
                pass
        
        # Update in-memory cache
        portfolio = self._get_user_portfolio_map(user_id)
        if holding is None:
            portfolio.pop(symbol, None)
        else:
            portfolio[symbol] = holding

    # ---------- Trades ----------

    def get_trades(self, user_id: str) -> List[Trade]:
        """Get all trades for a user."""
        if self.use_aws:
            try:
                resp = self._trades_table.query(
                    KeyConditionExpression=Key("user_id").eq(user_id)
                )
                trades = []
                for item in resp.get("Items", []):
                    trades.append(
                        Trade(
                            trade_id=item["trade_id"],
                            user_id=item["user_id"],
                            symbol=item["symbol"],
                            side=item["side"],
                            quantity=int(item["quantity"]),
                            price=float(item["price"]),
                            amount=float(item["amount"]),
                            timestamp=item["timestamp"],
                        )
                    )
                # Sort by timestamp descending
                trades.sort(key=lambda t: t.timestamp, reverse=True)
                return trades
            except ClientError:
                pass
        
        # In-memory fallback
        return self._trades.get(user_id, [])

    def execute_trade(
        self, user: User, symbol: str, side: str, qty: int, price: float
    ) -> Trade:
        """Execute a buy/sell trade."""
        amount = price * qty
        portfolio = self._get_user_portfolio_map(user.user_id)

        if side == "BUY":
            if user.cash_balance < amount:
                raise ValueError("Insufficient cash balance")
            user.cash_balance -= amount

            existing = portfolio.get(symbol)
            if existing:
                new_qty = existing.quantity + qty
                new_avg = (existing.quantity * existing.avg_buy_price + amount) / new_qty
                holding = Holding(
                    symbol=symbol, quantity=new_qty, avg_buy_price=round(new_avg, 2)
                )
            else:
                holding = Holding(
                    symbol=symbol, quantity=qty, avg_buy_price=round(price, 2)
                )
            self._update_portfolio(user.user_id, symbol, holding)

        elif side == "SELL":
            existing = portfolio.get(symbol)
            if not existing or existing.quantity < qty:
                raise ValueError("Insufficient holdings")
            user.cash_balance += amount
            new_qty = existing.quantity - qty
            if new_qty == 0:
                self._update_portfolio(user.user_id, symbol, None)
            else:
                holding = Holding(
                    symbol=symbol,
                    quantity=new_qty,
                    avg_buy_price=existing.avg_buy_price,
                )
                self._update_portfolio(user.user_id, symbol, holding)
        else:
            raise ValueError("side must be BUY or SELL")

        self.update_user(user)

        # Create trade record
        trade_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat() + "Z"
        trade = Trade(
            trade_id=trade_id,
            user_id=user.user_id,
            symbol=symbol,
            side=side,
            quantity=qty,
            price=round(price, 2),
            amount=round(amount, 2),
            timestamp=timestamp,
        )

        # Save trade
        if self.use_aws:
            try:
                self._trades_table.put_item(
                    Item={
                        "user_id": user.user_id,
                        "timestamp_trade_id": f"{timestamp}#{trade_id}",
                        "trade_id": trade_id,
                        "symbol": symbol,
                        "side": side,
                        "quantity": qty,
                        "price": round(price, 2),
                        "amount": round(amount, 2),
                        "timestamp": timestamp,
                    }
                )
            except ClientError:
                pass

        # Update in-memory cache
        if user.user_id not in self._trades:
            self._trades[user.user_id] = []
        self._trades[user.user_id].append(trade)

        # Publish to SNS
        self._publish_trade_to_sns(trade, user)

        return trade

    def _publish_trade_to_sns(self, trade: Trade, user: User) -> None:
        """Publish trade confirmation to SNS."""
        if not self.use_aws or not self._trade_topic_arn:
            return

        message = (
            f"Trade Confirmation\n\n"
            f"User: {user.email}\n"
            f"Action: {trade.side}\n"
            f"Symbol: {trade.symbol}\n"
            f"Quantity: {trade.quantity}\n"
            f"Price: ${trade.price:.2f}\n"
            f"Total Amount: ${trade.amount:.2f}\n"
            f"Time: {trade.timestamp}\n"
            f"Remaining Balance: ${user.cash_balance:.2f}"
        )

        try:
            self._sns.publish(
                TopicArn=self._trade_topic_arn,
                Subject=f"Virtual Trade Confirmation - {trade.side} {trade.symbol}",
                Message=message,
            )
        except (BotoCoreError, ClientError) as e:
            # Log error but don't fail the trade
            print(f"SNS publish failed: {e}")

    # ---------- Stocks ----------

    def _random_walk(self, price: float) -> float:
        """Simulate price movement."""
        import random
        delta = (random.random() - 0.5) * 2.0  # -1 to +1
        new_price = max(1.0, price + delta)
        return round(new_price, 2)

    def get_all_stocks(self, query: str = "") -> List[Dict]:
        """Get all stocks matching query."""
        query_upper = query.upper()
        result = []
        for stock in self._stocks.values():
            # Simulate price update
            stock["price"] = self._random_walk(stock["price"])
            if (
                not query_upper
                or query_upper in stock["symbol"]
                or query_upper in stock["companyName"].upper()
            ):
                result.append(stock.copy())
        return result

    def get_stock(self, symbol: str) -> Optional[Dict]:
        """Get single stock by symbol."""
        stock = self._stocks.get(symbol.upper())
        if not stock:
            return None
        stock["price"] = self._random_walk(stock["price"])
        return stock.copy()

    # ---------- Admin Helpers ----------

    def admin_get_all_users(self) -> List[User]:
        """Get all users (admin only)."""
        if self.use_aws:
            try:
                resp = self._users_table.scan()
                users = []
                for item in resp.get("Items", []):
                    users.append(
                        User(
                            user_id=item["user_id"],
                            email=item["email"],
                            password=item["password"],
                            cash_balance=float(item["cash_balance"]),
                        )
                    )
                return users
            except ClientError:
                pass
        
        return list(self._users.values())

    def admin_get_all_trades(self) -> List[Trade]:
        """Get all trades across all users (admin only)."""
        if self.use_aws:
            try:
                resp = self._trades_table.scan()
                trades = []
                for item in resp.get("Items", []):
                    trades.append(
                        Trade(
                            trade_id=item["trade_id"],
                            user_id=item["user_id"],
                            symbol=item["symbol"],
                            side=item["side"],
                            quantity=int(item["quantity"]),
                            price=float(item["price"]),
                            amount=float(item["amount"]),
                            timestamp=item["timestamp"],
                        )
                    )
                trades.sort(key=lambda t: t.timestamp, reverse=True)
                return trades
            except ClientError:
                pass
        
        all_trades = []
        for user_trades in self._trades.values():
            all_trades.extend(user_trades)
        all_trades.sort(key=lambda t: t.timestamp, reverse=True)
        return all_trades
