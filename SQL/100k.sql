CREATE TABLE #indexProcessed (
    "Index" VARCHAR(255),
    "Date" VARCHAR(255),
    "Open" FLOAT,
    High FLOAT,
    Low FLOAT,
    "Close" FLOAT,
    "Adj Close" FLOAT,
    Volume FLOAT,
    CloseUSD FLOAT
)

CREATE TABLE #indexProcessed_sorted (
    "Index" VARCHAR(255),
    "Date" VARCHAR(255),
    "Open" FLOAT,
    High FLOAT,
    Low FLOAT,
    "Close" FLOAT,
    "Adj Close" FLOAT,
    Volume FLOAT,
    CloseUSD FLOAT
)

CREATE TABLE #indexData (
    "Index" VARCHAR(255),
    "Name" VARCHAR(255),
    "Location" VARCHAR(255),
    "Currency" VARCHAR(255)
)

CREATE TABLE #indexList (
    "Index" VARCHAR(255)
)

-- Filter High > 5000
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT * 
INTO dbo.indexProcessed_temp
FROM #indexProcessed WHERE High > 5000

-- Filter by string	Index = NYA
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT * 
INTO dbo.indexProcessed_temp
FROM #indexProcessed WHERE "Index" = 'NYA'

-- Distinct	column Index
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT DISTINCT "Index"
INTO dbo.indexProcessed_temp
FROM #indexProcessed 

-- Sort	by High asc
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT *
INTO dbo.indexProcessed_temp
FROM #indexProcessed 
ORDER BY High ASC

-- Union with sorted
TRUNCATE TABLE #indexProcessed;
TRUNCATE TABLE #indexProcessed_sorted;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #indexProcessed_sorted
FROM '/100k/indexProcessed_sorted.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT x.*
INTO dbo.indexProcessed_temp
FROM (
    SELECT * FROM #indexProcessed
    UNION ALL
    SELECT * FROM #indexProcessed_sorted
) x


-- Group By	by Index Open min max avg sum count
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT "Index", 
MIN("Open") AS "Open_min", 
MAX("Open") AS "Open_max", 
AVG("Open") AS "Open_avg", 
SUM("Open") AS "Open_sum", 
COUNT(*) AS "Count"
INTO dbo.indexProcessed_temp
FROM #indexProcessed
GROUP BY "Index"

-- Left Join with indexData (take everything)
TRUNCATE TABLE #indexProcessed;
TRUNCATE TABLE #indexData;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #indexData
FROM '/100k/indexData.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT a.*, b.Name, b.Location, b.Currency
INTO dbo.indexProcessed_temp
FROM #indexProcessed a LEFT JOIN #indexData b ON a."Index" = b."Index"

-- Inner Join with index list
TRUNCATE TABLE #indexProcessed;
TRUNCATE TABLE #indexList;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #indexList
FROM '/100k/indexList.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT a.*
INTO dbo.indexProcessed_temp
FROM #indexProcessed a INNER JOIN #indexList b ON a."Index" = b."Index"

-- Math	High +-/* Low
TRUNCATE TABLE #indexProcessed;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

SELECT *,
 High + Low AS "Calculation1", 
 High - Low AS "Calculation2", 
 High * Low AS "Calculation3", 
 High / Low AS "Calculation4"
INTO dbo.indexProcessed_temp
FROM #indexProcessed

-- Pipeline

TRUNCATE TABLE #indexProcessed;
TRUNCATE TABLE #indexList;
TRUNCATE TABLE #indexData;
DROP TABLE IF EXISTS dbo.indexProcessed_temp;

BULK INSERT #indexProcessed
FROM '/100k/indexProcessed.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #indexList
FROM '/100k/indexList.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)

BULK INSERT #indexData
FROM '/100k/indexData.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)


SELECT x."Index", x."MarketcapUSD", x."MarketcapLocalCurrency", x."Date",
    MAX("MarketcapUSD") OVER (PARTITION BY "Date") AS "MaxMarketcapUSD"
INTO dbo.indexProcessed_temp
FROM (
    SELECT a."Index", "Open", High, Low, "Close", "Adj Close", Volume, CloseUSD,
        Volume * CloseUSD AS "MarketcapUSD", 
        Volume * "Close" AS "MarketcapLocalCurrency", 
        CAST("Date" AS DATE) AS "Date"
    FROM #indexProcessed a
        INNER JOIN #indexList b ON a."Index" = b."Index"
    WHERE Volume != 0
) x
LEFT JOIN #indexData y ON x."Index" = y."Index"
ORDER BY "Date" DESC