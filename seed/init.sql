CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.coins(
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(50) NOT NULL
)
DISTSTYLE ALL 
SORTKEY(ticker);


CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.prices(
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL distkey,
    qty_low FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    qty_high FLOAT NOT NULL,
    volume FLOAT NOT NULL
)
COMPOUND SORTKEY(ticker, date);

CREATE TABLE IF NOT EXISTS nicolas_ezequiel_arias300_coderhouse.exchanges(
    name VARCHAR(50) NOT NULL,
    url VARCHAR(100) NOT NULL
) 
DISTSTYLE ALL
SORTKEY (name);
