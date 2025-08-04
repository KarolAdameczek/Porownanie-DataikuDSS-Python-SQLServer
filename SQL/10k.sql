CREATE TABLE #companiesmarketcap (
    "Rank" INT,
    "Company Names" VARCHAR(255),
    "Company Code" VARCHAR(255),
    "Marketcap" FLOAT,
    "Stock Price" FLOAT,
    "Country" VARCHAR(255)
)

CREATE TABLE #companiesmarketcap_sorted (
    "Rank" INT,
    "Company Names" VARCHAR(255),
    "Company Code" VARCHAR(255),
    "Marketcap" FLOAT,
    "Stock Price" FLOAT,
    "Country" VARCHAR(255)
)

CREATE TABLE #countries (
    Country VARCHAR(255)
)

CREATE TABLE #countries_continents (
    Continent VARCHAR(255),
    Country VARCHAR(255)
)

--Filter by number	Stock Price > 20
TRUNCATE TABLE #companiesmarketcap--dbo.companiesmarketcap
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp

BULK INSERT #companiesmarketcap--dbo.companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
)


SELECT * 
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap--dbo.companiesmarketcap 
WHERE "Stock Price" > 20  

--Filter by string	Country = USA
TRUNCATE TABLE #companiesmarketcap;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT * 
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap
WHERE Country = 'USA'


--Distinct	Country
TRUNCATE TABLE #companiesmarketcap
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT DISTINCT Country
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap
 

--Sort	Stock Price asc
TRUNCATE TABLE #companiesmarketcap
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT * 
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap
ORDER BY "Stock Price" ASC

--Union	with Sorted
TRUNCATE TABLE #companiesmarketcap
TRUNCATE TABLE #companiesmarketcap_sorted;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

BULK INSERT #companiesmarketcap_sorted
FROM '/10k/companiesmarketcap_sorted.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT x.*
INTO dbo.companiesmarketcap_temp
FROM (
    SELECT * FROM #companiesmarketcap 
    UNION ALL
    SELECT * FROM #companiesmarketcap_sorted) x

--Group By	by Country, Marketcap min max avg sum + count
TRUNCATE TABLE #companiesmarketcap
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT Country, 
MIN(Marketcap) AS "Marketcap_min", 
MAX(Marketcap) AS "Marketcap_max", 
AVG(Marketcap) AS "Marketcap_avg", 
SUM(Marketcap) AS "Marketcap_sum", 
COUNT(*) AS "Count" 
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap
GROUP BY Country


--Left Join	with continents dataset
TRUNCATE TABLE #companiesmarketcap
TRUNCATE TABLE #countries_continents;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

BULK INSERT #countries_continents
FROM '/10k/Countries_Continents.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT a.*, b.Continent
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap a
LEFT JOIN #countries_continents b ON a.Country = b.Country


--Inner Join	with list of countries
TRUNCATE TABLE #companiesmarketcap;
TRUNCATE TABLE #countries;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

BULK INSERT #countries
FROM '/10k/CountryList.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT a.*
INTO dbo.companiesmarketcap_temp 
FROM #companiesmarketcap a
INNER JOIN #countries b ON a.Country = b.Country

--Math	Marketcap +-/* Stock Price
TRUNCATE TABLE #companiesmarketcap;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT *, Marketcap + "Stock Price" AS "Calculation1", 
Marketcap - "Stock Price" AS "Calculation2", 
Marketcap * "Stock Price" AS "Calculation3", 
Marketcap / "Stock Price" AS "Calculation4" 
INTO dbo.companiesmarketcap_temp
FROM #companiesmarketcap


-- =====================================================

-- Pipeline

CREATE TABLE #pipeline10k_temp (
    "Rank" INT,
    "Company Names" VARCHAR(255),
    "Company Code" VARCHAR(255),
    "Marketcap" FLOAT,
    "Stock Price" FLOAT,
    "Country" VARCHAR(255)
)

TRUNCATE TABLE #companiesmarketcap;
TRUNCATE TABLE #countries_continents;
DROP TABLE IF EXISTS #pipeline10k_temp;
DROP TABLE IF EXISTS dbo.companiesmarketcap_temp;

-- -- -- -- 

BULK INSERT #companiesmarketcap
FROM '/10k/companiesmarketcap_cleaned.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);


BULK INSERT #countries_continents
FROM '/10k/Countries_Continents.csv'
WITH
(
    FORMAT = 'CSV',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    KEEPNULLS 
);

SELECT Rank, 
    REPLACE("Company Names", CHAR(10), '') AS  "Company Names",
    "Company Code"
    ,CASE 
        WHEN Marketcap LIKE 'T %' THEN CAST(REPLACE(Marketcap, 'T $', '') AS FLOAT) * 1000000000000
        WHEN Marketcap LIKE 'B %' THEN CAST(REPLACE(Marketcap, 'B $', '') AS FLOAT) * 1000000000
        WHEN Marketcap LIKE 'M %' THEN CAST(REPLACE(Marketcap, 'M $', '') AS FLOAT) * 1000000
        ELSE CAST(REPLACE(Marketcap, '$', '') AS FLOAT)
    END AS Marketcap
    , CAST(REPLACE("Stock Price", ',', '') AS FLOAT) AS "Stock Price"
    , Country
INTO #pipeline10k_temp
FROM #companiesmarketcap 
WHERE "Country" IS NOT NULL AND "Marketcap" IS NOT NULL AND "Stock Price" IS NOT NULL
    AND "Marketcap" > 0 AND "Stock Price" > 0



SELECT a.*, b."Country_Marketcap_max", b."Country_Marketcap_avg", b."Country_Marketcap_sum", c.Continent,
    "Marketcap" / b."Country_Marketcap_sum" AS "MarketcapVsCouuntrySum",
    "Marketcap" / b."Country_Marketcap_max" AS "MarketcapVsCouuntryBiggest",
    "Marketcap" / b."Country_Marketcap_avg" AS "MarketcapVsCouuntryAverage"
INTO dbo.companiesmarketcap_temp
FROM #pipeline10k_temp a
LEFT JOIN (
    SELECT Country, 
        MAX(Marketcap) AS "Country_Marketcap_max", 
        AVG(Marketcap) AS "Country_Marketcap_avg", 
        SUM(Marketcap) AS "Country_Marketcap_sum"
    FROM #pipeline10k_temp
    GROUP BY Country) b ON a.Country = b.Country
LEFT JOIN #countries_continents c ON a.Country = c.Country
ORDER BY c.Continent ASC, a.Country ASC, a.Marketcap DESC;