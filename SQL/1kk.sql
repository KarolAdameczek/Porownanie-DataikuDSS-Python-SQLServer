CREATE TABLE #sp500_stocks (
    "Date" VARCHAR(255),
    Symbol VARCHAR(255),
    "Adj Close" FLOAT,
    "Close" FLOAT,
    "High" FLOAT,
    "Low" FLOAT,
    "Open" FLOAT,
    Volume FLOAT
)

CREATE TABLE #sp500_stocks_sorted (
    "Date" VARCHAR(255),
    Symbol VARCHAR(255),
    "Adj Close" FLOAT,
    "Close" FLOAT,
    "High" FLOAT,
    "Low" FLOAT,
    "Open" FLOAT,
    Volume FLOAT
)

CREATE TABLE #sp500_companies (
    Exchange VARCHAR(255),
    Symbol VARCHAR(255),
    Shortname VARCHAR(255),
    Longname VARCHAR(255),
    Sector VARCHAR(255),
    Industry VARCHAR(255),
    Currentprice FLOAT,
    Marketcap FLOAT,
    Ebitda FLOAT,
    Revenuegrowth FLOAT,
    City VARCHAR(255),
    "State" VARCHAR(255),
    Country VARCHAR(255),
    Fulltimeemployees FLOAT,
    Longbusinesssummary VARCHAR(8000),
    "Weight" FLOAT
)

CREATE TABLE #sp500_company_list (
    Symbol VARCHAR(255)
)

-- Filter by number	Open > 70
TRUNCATE TABLE #sp500_stocks;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT * 
INTO dbo.sp500_temp
FROM #sp500_stocks WHERE "Open" > 70

-- Filter by string	Symbol = AAPL
TRUNCATE TABLE #sp500_stocks;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT * 
INTO dbo.sp500_temp
FROM #sp500_stocks WHERE Symbol = 'AAPL'

-- Distinct Symbol
TRUNCATE TABLE #sp500_stocks;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT DISTINCT Symbol
INTO dbo.sp500_temp
FROM #sp500_stocks 

-- Sort	by Open asc
TRUNCATE TABLE #sp500_stocks;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT *
INTO dbo.sp500_temp
FROM #sp500_stocks 
ORDER BY "Open" ASC

-- Union with Sorted
TRUNCATE TABLE #sp500_stocks;
TRUNCATE TABLE #sp500_stocks_sorted;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #sp500_stocks_sorted
FROM '/1kk/sp500_stocks_sorted.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)


SELECT x.*
INTO dbo.sp500_temp
FROM (
    SELECT * FROM #sp500_stocks
    UNION ALL
    SELECT * FROM #sp500_stocks_sorted
) x


-- Group By	by Symbol Volume min max avg sum + count
TRUNCATE TABLE #sp500_stocks
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT Symbol, 
MIN(Volume) AS Volume_min, 
MAX(Volume) AS Volume_max, 
AVG(Volume) AS Volume_avg, 
SUM(Volume) AS Volume_sum,  
COUNT(*) AS "Count"
INTO dbo.sp500_temp
FROM #sp500_stocks
GROUP BY Symbol

-- Left Join with companies take everything
TRUNCATE TABLE #sp500_stocks;
TRUNCATE TABLE #sp500_companies;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #sp500_companies
FROM '/1kk/sp500_companies.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT a.*, b.Exchange, 
    b.Shortname, b.Longname, b.Sector, 
    b.Industry, b.Currentprice, b.Marketcap,
    b.Ebitda, b.Revenuegrowth, b.City, 
    b."State", b.Country, b.Fulltimeemployees, 
    b.Longbusinesssummary, b."Weight"
INTO dbo.sp500_temp
FROM #sp500_stocks a
LEFT JOIN #sp500_companies b
ON a.Symbol = b.Symbol

-- Inner Join with company list
TRUNCATE TABLE #sp500_stocks;
TRUNCATE TABLE #sp500_company_list;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #sp500_company_list
FROM '/1kk/sp500_company_list.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT a.*
INTO dbo.sp500_temp
FROM #sp500_stocks a
INNER JOIN #sp500_company_list b
ON a.Symbol = b.Symbol

-- Math High +-/* Low
TRUNCATE TABLE #sp500_stocks;
DROP TABLE IF EXISTS dbo.sp500_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT *,
    "High" + "Low" AS "Calculation1",
    "High" - "Low" AS "Calculation2",
    "High" * "Low" AS "Calculation3",
    "High" / "Low" AS "Calculation4"
INTO dbo.sp500_temp
FROM #sp500_stocks 


-- Pipeline
CREATE TABLE #pipeline1kk_temp (
    Symbol VARCHAR(255),
    "Adj Close" FLOAT,
    "Close" FLOAT,
    "High" FLOAT,
    "Low" FLOAT,
    "Open" FLOAT,
    Volume FLOAT,
    "Date" DATE,
    "OpenCloseDiff" FLOAT,
    Exchange VARCHAR(255),
    Sector VARCHAR(255),
    Industry VARCHAR(255),
    Country VARCHAR(255)
)

TRUNCATE TABLE #sp500_stocks;
TRUNCATE TABLE #sp500_companies;
TRUNCATE TABLE #sp500_company_list;
DROP TABLE IF EXISTS dbo.sp500_temp;
DROP TABLE IF EXISTS #pipeline1kk_temp;

BULK INSERT #sp500_stocks
FROM '/1kk/sp500_stocks.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #sp500_companies
FROM '/1kk/sp500_companies.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #sp500_company_list
FROM '/1kk/sp500_company_list.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)


SELECT a.Symbol, "Adj Close", "Close", "High", "Low", "Open", Volume,
        CAST("Date" AS DATE) AS "Date",
        "Open" - "Close" AS "OpenCloseDiff",
        b.Exchange,
        b.Sector,
        b.Industry,
        b.Country
    INTO #pipeline1kk_temp
    FROM #sp500_stocks a
    LEFT JOIN #sp500_companies b ON a.Symbol = b.Symbol
    WHERE "Date" > '2015-01-01'


SELECT x.*
INTO dbo.sp500_temp
FROM (
    SELECT Country AS "Name", MAX("High") AS "High_max", MIN("Low") AS "Low_min", MAX("OpenCloseDiff") AS "OpenCloseDiff_max", AggregationType = 'Country'
    FROM #pipeline1kk_temp
    GROUP BY Country
    UNION
    SELECT Exchange AS "Name", MAX("High") AS "High_max", MIN("Low") AS "Low_min", MAX("OpenCloseDiff") AS "OpenCloseDiff_max", AggregationType = 'Exchange'
    FROM #pipeline1kk_temp
    GROUP BY Exchange
    UNION
    SELECT Sector AS "Name", MAX("High") AS "High_max", MIN("Low") AS "Low_min", MAX("OpenCloseDiff") AS "OpenCloseDiff_max", AggregationType = 'Sector'
    FROM #pipeline1kk_temp
    GROUP BY Sector
    UNION
    SELECT Industry AS "Name", MAX("High") AS "High_max", MIN("Low") AS "Low_min", MAX("OpenCloseDiff") AS "OpenCloseDiff_max", AggregationType = 'Industry'
    FROM #pipeline1kk_temp
    GROUP BY Industry
    UNION
    SELECT Symbol AS "Name", MAX("High") AS "High_max", MIN("Low") AS "Low_min", MAX("OpenCloseDiff") AS "OpenCloseDiff_max", AggregationType = 'Company'
    FROM #pipeline1kk_temp
    GROUP BY Symbol
) x
ORDER BY AggregationType ASC, "Name" ASC

