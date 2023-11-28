import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import hashlib
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load the .env file
load_dotenv()

REDSHIFT_SCHEMA = os.getenv('REDSHIFT_SCHEMA')


def connect_to_db():
    try:
        db = {
            "host": os.getenv('REDSHIFT_ENDPOINT'),
            "port": os.getenv('REDSHIFT_PORT'),
            "dbname": os.getenv('REDSHIFT_DATABASE'),
            "user": os.getenv('REDSHIFT_USER'),
            "pwd": os.getenv('REDSHIFT_PASSWD'),
            "schema": os.getenv('REDSHIFT_SCHEMA')

        }

        logging.info("Conectándose a la base de datos...")
        engine = create_engine(
            f"postgresql://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}", connect_args={"options": f"-c search_path={db['schema']}"}
        )

        logging.info(
            "Conexión a la base de datos establecida exitosamente")
        return engine

    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None


def load_to_sql(df, table_name, engine, if_exists="replace"):
    try:
        logging.info("Cargando datos en la base de datos...")
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        logging.info("Datos cargados exitosamente en la base de datos")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")


def generate_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def insert_trades_without_duplicates(engine, df_crypto_trading):
    with engine.connect() as conn, conn.begin():
        conn.execute(
            f"TRUNCATE TABLE {REDSHIFT_SCHEMA}.stg_crypto_trading")

        # Load the DataFrame df_crypto_trading into the fact_crypto_trading table
        load_to_sql(df_crypto_trading,
                    "stg_crypto_trading", conn, "append")

        # Update existing records and insert new records into the fact_crypto_trading table

        conn.execute("""
            MERGE INTO nicolas_ezequiel_arias300_coderhouse.fact_crypto_trading
            USING (SELECT * FROM stg_crypto_trading) AS stg_crypto_trading
            ON nicolas_ezequiel_arias300_coderhouse.fact_crypto_trading.trading_id = stg_crypto_trading.trading_id
            WHEN MATCHED THEN
                UPDATE SET
                    high = stg_crypto_trading.high,
                    low = stg_crypto_trading.low,
                    qty_low = stg_crypto_trading.qty_low,
                    qty_high = stg_crypto_trading.qty_high,
                    volume = stg_crypto_trading.volume    
                     
            WHEN NOT MATCHED THEN
                INSERT (trading_id, coin_id, exchange_id, date, time, qty_low, high, low, qty_high, volume, exchange_trade_id)
                VALUES (stg_crypto_trading.trading_id, stg_crypto_trading.coin_id, stg_crypto_trading.exchange_id, stg_crypto_trading.date, stg_crypto_trading.time, stg_crypto_trading.qty_low, stg_crypto_trading.high, stg_crypto_trading.low, stg_crypto_trading.qty_high, stg_crypto_trading.volume, stg_crypto_trading.exchange_trade_id)
            """)


def insert_dates_without_duplicates(engine, df_dates):
    with engine.connect() as conn, conn.begin():
        conn.execute(
            f"TRUNCATE TABLE {REDSHIFT_SCHEMA}.stg_dim_dates")

        # Load the DataFrame df_dates into the dim_dates table
        load_to_sql(df_dates,
                    "stg_dim_dates", conn, "append")

        # Update existing records and insert new records into the dim_dates table
        conn.execute("""
            MERGE INTO nicolas_ezequiel_arias300_coderhouse.dim_dates
            USING (SELECT * FROM stg_dim_dates) AS stg_dim_dates
            ON nicolas_ezequiel_arias300_coderhouse.dim_dates.date = stg_dim_dates.date
            WHEN MATCHED THEN
                UPDATE SET
                    day = stg_dim_dates.day,
                    month = stg_dim_dates.month,
                    year = stg_dim_dates.year         
            
            WHEN NOT MATCHED THEN
                INSERT (date, day, month, year)
                VALUES (stg_dim_dates.date, stg_dim_dates.day, stg_dim_dates.month, stg_dim_dates.year)
            """)
