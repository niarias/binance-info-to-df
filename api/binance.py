import requests
from api.mapper.data_mapper import DataMapper

def get_24hr_ticker(symbol):
    base_url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {
        "symbol": symbol
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return DataMapper.extract_data(data)
    else:
        return response.text


def get_binance_pairs(whitelist):
    binance_endpoint = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(binance_endpoint)

    # Comprobar si la respuesta es exitosa
    if response.status_code != 200:
        print("¡Error al obtener datos de Binance!")
        return

    data = response.json()

    whitelisted_pairs = []

    for symbol in data['symbols']:
        if symbol['status'] == 'TRADING':
            base_asset = symbol['baseAsset']
            quote_asset = symbol['quoteAsset']

            # Filtrar sólo los pares donde ambos, base_asset y quote_asset, estén en la whitelist
            if base_asset in whitelist and quote_asset in whitelist:
                whitelisted_pairs.append(base_asset + quote_asset)

    return whitelisted_pairs





