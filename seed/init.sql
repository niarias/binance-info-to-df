CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_coins (
    coin_id INT IDENTITY(1,1) PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(50) NOT NULL
)
DISTSTYLE ALL 
SORTKEY(ticker);

CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_exchanges (
    exchange_id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    url VARCHAR(100) NOT NULL
) 
DISTSTYLE ALL
SORTKEY(name);


CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.fact_crypto_trading (
    trading_id VARCHAR(100) PRIMARY KEY,
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


CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.stg_crypto_trading (
    trading_id VARCHAR(100) PRIMARY KEY,
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


CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_date (
    date VARCHAR(100) PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
)
DISTSTYLE ALL 
SORTKEY(date);



CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.stg_dim_dates (
    date VARCHAR(100) PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL
)
DISTSTYLE ALL 
SORTKEY(date);
