# Binance Ticker Data Fetcher

This project is designed to fetch 24-hour ticker data for specified trading pairs from Binance and present it in a pandas DataFrame. By default, it fetches data for the trading pairs: BTC, ETH, BNB, and LTC.

## Prerequisites

Python 3.x
An active internet connection
Installation

## Installation

python -m venv venv
source venv/bin/activate # On Windows use: .\venv\Scripts\activate

pip install -r requirements.txt

## Execution

Once you have installed the required packages, you can execute the script as follows:

python main.py

## Usage

Upon execution, the script will fetch the 24-hour ticker data for the trading pairs specified in the whitelist and display the DataFrame in the console. The DataFrame will contain all the ticker information along with a 'pair' column indicating the trading pair.

## Customization

To fetch data for trading pairs other than the default ones, modify the whitelist list in the script with the desired trading pairs.
