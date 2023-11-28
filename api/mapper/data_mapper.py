from datetime import datetime


class DataMapper:
    @staticmethod
    def extract_data(data):
        date = datetime.utcfromtimestamp(
            data['openTime'] / 1000).strftime('%Y-%m-%d')

        time = datetime.utcfromtimestamp(
            data['openTime'] / 1000).strftime('%H:%M:%S')
        return {
            "high": data["highPrice"],
            "low": data["lowPrice"],
            "qty_low": data["lastQty"],
            "qty_high": data["lastQty"],
            "volume": data["volume"],
            "date": date,
            "time": time,
            "exchange_trade_id": data["lastId"]
        }
