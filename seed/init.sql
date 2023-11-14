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
    trading_id VARCHAR(36) PRIMARY KEY,,
    coin_id INT NOT NULL,
    exchange_id INT NOT NULL,
    date_id INT NOT NULL DISTKEY,
    qty_low FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    qty_high FLOAT NOT NULL,
    volume FLOAT NOT NULL
)
COMPOUND SORTKEY(coin_id, exchange_id, date_id);

CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.dim_date (
    date_id VARCHAR(36) PRIMARY KEY,
    date DATETIME NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
)
DISTSTYLE ALL 
SORTKEY(date);