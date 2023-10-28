import pandas as pd
from api.binance import get_24hr_ticker, get_binance_pairs

whitelist = ["BTC", "ETH", "BNB", "LTC"]

def generate_dataframe():
    try:
        pairs = get_binance_pairs(whitelist)
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

if __name__ == "__main__":
    df = generate_dataframe()
    if df is not None:
        print(df)
