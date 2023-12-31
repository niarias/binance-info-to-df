import psycopg2
import os
from dotenv import load_dotenv
from const.data import whitelist, exchanges
from helpers.utils import connect_to_db, load_to_sql
import pandas as pd
# Load the .env file
load_dotenv()

# Access the variables
REDSHIFT_ENDPOINT = os.getenv('REDSHIFT_ENDPOINT')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWD = os.getenv('REDSHIFT_PASSWD')
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')


def create_tables():
    # Form the connection string
    conn_string = f"dbname='{REDSHIFT_DATABASE}' port='{REDSHIFT_PORT}' user='{REDSHIFT_USER}' password='{REDSHIFT_PASSWD}' host='{REDSHIFT_ENDPOINT}'"

    # Connect to Redshift
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cur:
            # Create coins table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_coins (
                    coin_id INT IDENTITY(1,1) PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    name VARCHAR(50) NOT NULL
                )
                DISTSTYLE ALL 
                SORTKEY(ticker);
            """)
            print("coins table created successfully!")

            # Create prices table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_exchanges (
                    exchange_id INT IDENTITY(1,1) PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    url VARCHAR(100) NOT NULL
                ) 
                DISTSTYLE ALL
                SORTKEY(name);
            """)
            print("prices table created successfully!")

            # Create exchanges table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.fact_crypto_trading (
                trading_id VARCHAR(100) PRIMARY KEY,,
                coin_id INT NOT NULL,
                exchange_id INT NOT NULL,
                date VARCHAR(30) NOT NULL DISTKEY,
                time VARCHAR(30) NOT NULL,
                qty_low FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                qty_high FLOAT NOT NULL,
                volume FLOAT NOT NULL,
                exchange_trade_id VARCHAR(100) NOT NULL
            )
            COMPOUND SORTKEY(coin_id, exchange_id, date);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.stg_crypto_trading (
                trading_id VARCHAR(100) PRIMARY KEY,,
                coin_id INT NOT NULL,
                exchange_id INT NOT NULL,
                date VARCHAR(30) NOT NULL DISTKEY,
                time VARCHAR(30) NOT NULL,
                qty_low FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                qty_high FLOAT NOT NULL,
                volume FLOAT NOT NULL,
                exchange_trade_id VARCHAR(100) NOT NULL
            )
            COMPOUND SORTKEY(coin_id, exchange_id, date);
            """)
            print("exchanges table created successfully!")

            cur.execute("""
               CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_date (
                    date VARCHAR(100) PRIMARY KEY,
                    day INT NOT NULL,
                    month INT NOT NULL,
                    year INT NOT NULL
                )
                DISTSTYLE ALL 
                SORTKEY(date);
            """)
            print("exchanges table created successfully!")

            cur.execute("""
               CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.stg_dim_dates (
                    date VARCHAR(100) PRIMARY KEY,
                    day INT NOT NULL,
                    month INT NOT NULL,
                    year INT NOT NULL
                )
                DISTSTYLE ALL 
                SORTKEY(date);
            """)
            print("exchanges table created successfully!")

    conn.close()


def insert_coin_into_db():
    df = pd.DataFrame(whitelist)
    engine = connect_to_db()
    load_to_sql(df, "dim_coins", engine)


def insert_exchange_into_db():
    df = pd.DataFrame(exchanges)
    engine = connect_to_db()
    load_to_sql(df, "dim_exchanges", engine)


if __name__ == '__main__':
    create_tables()
    insert_coin_into_db()
    insert_exchange_into_db()
