import pandas as pd
from api.binance import get_24hr_ticker
from helpers.utils import connect_to_db, load_to_sql, generate_hash
from const.data import whitelist, exchanges
import uuid


def generate_df_general():
    try:
        pairs = [coin['ticker'] for coin in whitelist]
    except Exception as e:
        print(f"Error fetching pairs: {e}")
        return

    data_list = []
    for pair in pairs:
        try:
            print(f"Fetching data for {pair}")
            pair_data = get_24hr_ticker(pair)
            pair_data['ticker'] = pair
            data_list.append(pair_data)
        except Exception as e:
            print(f"Error fetching data for {pair}: {e}")
            continue

    df = pd.DataFrame(data_list)
    return df


def generate_df_date(df_original):
    df = df_original.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    unique_dates = df['date'].dt.normalize().unique()
    dates_df = pd.DataFrame(unique_dates, columns=['date'])

    dates_df['day'] = dates_df['date'].dt.day
    dates_df['month'] = dates_df['date'].dt.month
    dates_df['year'] = dates_df['date'].dt.year

    dates_df['date_id'] = dates_df['date'].dt.strftime('%Y-%m-%d')
    dates_df['date_id'] = dates_df['date_id'].apply(generate_hash)

    return dates_df


def generate_df_trades(df_original):
    df = df_original.copy()
    df['exchange_id'] = 1
    df['coin_id'] = df['ticker'].map(
        {coin['ticker']: coin['coin_id'] for coin in whitelist})
    # Generate a new uuid
    df['trading_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    df['date_id'] = df['date']
    df['date_id'] = df['date_id'].apply(generate_hash)

    # Remove date column
    df.drop(columns=['date'], inplace=True)
    # Remove column ticker
    df.drop(columns=['ticker'], inplace=True)

    return df


def insert_into_db():
    engine = connect_to_db()
    if engine is not None:
        df = generate_df_general()

        df_dates = generate_df_date(df)
        df_trades = generate_df_trades(df)

        load_to_sql(df_dates, "dim_dates", engine, if_exists="append")
        load_to_sql(df_trades, "fact_crypto_trading",
                    engine, if_exists="append")


if __name__ == "__main__":
    insert_into_db()
