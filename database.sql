CREATE database Portfolio_db;

USE Portfolio_db;

CREATE TABLE stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    stock_symbol VARCHAR(10),
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT
);

desc stock_prices;

SELECT * FROM stock_prices
WHERE stock_symbol = 'AAPL';

SELECT DISTINCT stock_symbol FROM stock_prices;

SELECT stock_symbol, count(*) 
FROM stock_prices
GROUP BY stock_symbol
ORDER BY count(*) DESC;

SELECT stock_symbol, date, COUNT(*) 
FROM stock_prices
WHERE stock_symbol = 'AAPL'
GROUP BY stock_symbol, date
HAVING COUNT(*) > 1;

DELETE FROM stock_prices
WHERE id NOT IN (
    SELECT MIN(id) 
    FROM stock_prices
    GROUP BY stock_symbol, date
);

ALTER TABLE stock_prices 
ADD CONSTRAINT unique_stock_date UNIQUE (stock_symbol, date);

TRUNCATE stock_prices;

DESC stock_prices;

SELECT * FROM stock_prices;

SELECT stock_symbol, count(*) FROM stock_prices
GROUP BY stock_symbol
ORDER BY count(*) DESC;

SELECT 
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS missing_dates,
    SUM(CASE WHEN stock_symbol IS NULL THEN 1 ELSE 0 END) AS missing_symbols,
    SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) AS missing_open_prices,
    SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) AS missing_close_prices,
    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS missing_volumes
FROM stock_prices;

SELECT stock_symbol, date, COUNT(*) 
FROM stock_prices 
GROUP BY stock_symbol, date 
HAVING COUNT(*) > 1;

SELECT * FROM stock_prices
WHERE close_price > (SELECT AVG(close_price) + 3 * STDDEV(close_price) FROM stock_prices)
   OR close_price < (SELECT AVG(close_price) - 3 * STDDEV(close_price) FROM stock_prices);
   

SELECT stock_symbol, 
       date, 
       close_price,
       LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) AS previous_close,
       (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
        LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
FROM stock_prices
ORDER BY stock_symbol, date;

SELECT stock_symbol, 
       AVG(daily_return) AS avg_return, 
       STDDEV(daily_return) AS volatility
FROM (
    SELECT stock_symbol, 
           date, 
           (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
            LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
    FROM stock_prices
) AS subquery
GROUP BY stock_symbol
ORDER BY avg_return DESC;

SELECT a.stock_symbol AS stock1, 
       b.stock_symbol AS stock2, 
       (SUM(a.daily_return * b.daily_return) - (SUM(a.daily_return) * SUM(b.daily_return)) / COUNT(*)) /
       (SQRT(SUM(a.daily_return * a.daily_return) - (SUM(a.daily_return) * SUM(a.daily_return)) / COUNT(*)) * 
        SQRT(SUM(b.daily_return * b.daily_return) - (SUM(b.daily_return) * SUM(b.daily_return)) / COUNT(*))) 
       AS correlation
FROM (
    SELECT stock_symbol, 
           date, 
           (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
            LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
    FROM stock_prices
) a
JOIN (
    SELECT stock_symbol, 
           date, 
           (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
            LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
    FROM stock_prices
) b
ON a.date = b.date AND a.stock_symbol < b.stock_symbol
GROUP BY stock1, stock2
ORDER BY correlation DESC;

SELECT stock_symbol, 
       AVG(daily_return) / STDDEV(daily_return) AS sharpe_ratio
FROM (
    SELECT stock_symbol, 
           date, 
           (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
            LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
    FROM stock_prices
) AS subquery
GROUP BY stock_symbol
ORDER BY sharpe_ratio DESC;

WITH daily_returns AS (
    SELECT 
        stock_symbol,
        date,
        (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
        LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) AS daily_return
    FROM stock_prices
)

SELECT 
    stock_symbol,
    AVG(daily_return) * 252 AS annualized_return,  -- Annualizing returns
    STDDEV(daily_return) * SQRT(252) AS annualized_volatility,  -- Annualizing volatility
    (AVG(daily_return) * 252 - 0.032) / (STDDEV(daily_return) * SQRT(252)) AS sharpe_ratio  -- Correct Sharpe Ratio
FROM daily_returns
GROUP BY stock_symbol
ORDER BY sharpe_ratio DESC;

SELECT * FROM stock_prices;

ALTER TABLE stock_prices 
ADD COLUMN daily_return FLOAT;

WITH daily_return_cte AS (
    SELECT id, 
           (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
            LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
    FROM stock_prices
)
UPDATE stock_prices s
JOIN daily_return_cte d ON s.id = d.id
SET s.daily_return = d.daily_return;

SELECT * FROM stock_prices;

ALTER TABLE stock_prices 
ADD COLUMN avg_return FLOAT,
ADD COLUMN volatility FLOAT;

SET SQL_SAFE_UPDATES = 0;


WITH stock_returns AS (
    SELECT stock_symbol, 
           AVG(daily_return) AS avg_return, 
           STDDEV(daily_return) AS volatility
    FROM (
        SELECT stock_symbol, 
               date, 
               (close_price - LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date)) / 
                LAG(close_price) OVER (PARTITION BY stock_symbol ORDER BY date) * 100 AS daily_return
        FROM stock_prices
    ) AS subquery
    GROUP BY stock_symbol
)
UPDATE stock_prices s
JOIN stock_returns sr ON s.stock_symbol = sr.stock_symbol
SET s.avg_return = sr.avg_return,
    s.volatility = sr.volatility;

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM stock_prices;

ALTER TABLE stock_prices 
ADD COLUMN annualized_return FLOAT,
ADD COLUMN annualized_volatility FLOAT,
ADD COLUMN sharpe_ratio FLOAT;

SELECT * FROM stock_prices;




	

