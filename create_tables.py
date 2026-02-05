# create_tables.py
import os
import boto3
from botocore.exceptions import ClientError

REGION = os.getenv("AWS_REGION", "ap-south-1")

dynamodb = boto3.client("dynamodb", region_name=REGION)

def create_table_if_not_exists(**kwargs):
    name = kwargs["TableName"]
    try:
        dynamodb.describe_table(TableName=name)
        print(f"Table '{name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Creating table '{name}'...")
            dynamodb.create_table(**kwargs)
            waiter = dynamodb.get_waiter("table_exists")
            waiter.wait(TableName=name)
            print(f"Table '{name}' created.")
        else:
            raise

def main():
    # 1) Users table
    create_table_if_not_exists(
        TableName="virtual_trading_users",
        AttributeDefinitions=[
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "email", "KeyType": "HASH"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # 2) Portfolio table
    create_table_if_not_exists(
        TableName="virtual_trading_portfolio",
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "symbol", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "symbol", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # 3) Trades table
    create_table_if_not_exists(
        TableName="virtual_trading_trades",
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "timestamp_trade_id", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "timestamp_trade_id", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

if __name__ == "__main__":
    main()