
use `ELECTRONICA-DW`;

-- Q1) Present total sales of all products supplied by each supplier with respect to quarter and
-- month using drill down concept.
SELECT 
    SD.SupplierName,
    TD.Quarter,
    TD.Month,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN SUPPLIER_DIM SD ON TF.SupplierID = SD.SupplierID		-- get the supplier id to access their names
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_				-- get the time id to access the different granularity of times
GROUP BY 
    SD.SupplierName, 
    TD.Quarter, 
    TD.Month
ORDER BY 
    SD.SupplierName, 
    TD.Quarter, 
    TD.Month;


-- Q2) Find total sales of product with respect to month using feature of rollup on month and
-- 	feature of dicing on supplier with name "DJI" and Year as "2019". You will use the 
-- grouping sets feature to achieve rollup. Your output should be sequentially ordered
-- 	according to product and month.

-- SKIPPED 


-- Q3) Find the 5 most popular products sold over the weekends.

SELECT 
    PD.ProductName,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN PRODUCT_DIM PD ON TF.ProductID = PD.ProductID
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_
WHERE 
    DAYOFWEEK(TD.Date_) IN (1, 7) -- 1 for Sunday and 7 for Saturday
GROUP BY 
    PD.ProductName
ORDER BY 
    TotalSales DESC LIMIT 5;
    
-- Q4) Q4 Present the quarterly sales of each product for 2019 along with its total yearly sales.
-- Note: each quarter sale must be a column and yearly sale as well. Order result according to product

SELECT 
    PD.ProductName,
    SUM(CASE WHEN QUARTER(TD.Date_) = 1 AND YEAR(TD.Date_) = 2019 THEN TF.Sales ELSE 0 END) AS Q1_Sales,
    SUM(CASE WHEN QUARTER(TD.Date_) = 2 AND YEAR(TD.Date_) = 2019 THEN TF.Sales ELSE 0 END) AS Q2_Sales,
    SUM(CASE WHEN QUARTER(TD.Date_) = 3 AND YEAR(TD.Date_) = 2019 THEN TF.Sales ELSE 0 END) AS Q3_Sales,
    SUM(CASE WHEN QUARTER(TD.Date_) = 4 AND YEAR(TD.Date_) = 2019 THEN TF.Sales ELSE 0 END) AS Q4_Sales,
    SUM(CASE WHEN YEAR(TD.Date_) = 2019 THEN TF.Sales ELSE 0 END) AS Total_Yearly_Sales
FROM 
    Transactions_Fact TF
    INNER JOIN PRODUCT_DIM PD ON TF.ProductID = PD.ProductID
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_
GROUP BY 
    PD.ProductName
ORDER BY 
    PD.ProductName;


-- Q5)Find an anomaly in the data warehouse dataset. write a query to show the anomaly and explain the anomaly in your project report.
-- my query checks if a single supplier name owns multiple supplier ids
SELECT 
    SupplierName,
    COUNT(DISTINCT SupplierID) AS NumberOfUniqueIDs
FROM 
    SUPPLIER_DIM
GROUP BY 
    SupplierName
HAVING 
    COUNT(DISTINCT SupplierID) > 1;


-- Q6 Create a materialised view with the name “STOREANALYSIS_MV” that presents the product-wise sales analysis for each store.
-- STORE_ID PROD_ID STORE_TOTAL
-- ------------- ------------ -------------------
-- CANNOT CREATE MATERIALISED VIEW ON MYSQL SO WE CREATE A NORMAL VIEW
-- Drop the view if it exists
DROP VIEW IF EXISTS STOREANALYSIS_MV;

-- Create the view
CREATE VIEW STOREANALYSIS_MV AS
SELECT 
    TF.StoreID AS STORE_ID, 
    TF.ProductID AS PROD_ID, 
    SUM(TF.Sales) AS STORE_TOTAL
FROM 
    Transactions_Fact TF
GROUP BY 
    TF.StoreID, TF.ProductID;

-- Drop the table if it exists
DROP TABLE IF EXISTS STOREANALYSIS_TABLE;

-- Create the table
CREATE TABLE STOREANALYSIS_TABLE (
    STORE_ID INT,
    PROD_ID INT,
    STORE_TOTAL DECIMAL(10, 2)
);

-- Insert data into the table
INSERT INTO STOREANALYSIS_TABLE (STORE_ID, PROD_ID, STORE_TOTAL)
SELECT 
    STORE_ID, 
    PROD_ID, 
    STORE_TOTAL
FROM 
    STOREANALYSIS_MV;

-- Select data from the table
SELECT * FROM STOREANALYSIS_TABLE ORDER BY PROD_ID;

-- Q7 Use the concept of Slicing calculate the total sales for the store “Tech Haven”and product combination over the months.

SELECT 
    TF.ProductID AS PROD_ID,
    TD.Month,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN STORE_DIM SD ON TF.StoreID = SD.StoreID
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_
WHERE 
    SD.StoreName = 'Tech Haven'
GROUP BY 
    TF.ProductID, 
    TD.Month
ORDER BY 
    TF.ProductID, 
    TD.Month;


-- Q8 Create a materialized view named "SUPPLIER_PERFORMANCE_MV" that presents the monthly performance of each supplier.
DROP VIEW IF EXISTS SUPPLIER_PERFORMANCE_MV;
CREATE VIEW SUPPLIER_PERFORMANCE_MV AS
SELECT 
    TF.SupplierID,
    MONTH(TD.Date_) AS Month,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_
GROUP BY 
    TF.SupplierID, MONTH(TD.Date_), YEAR(TD.Date_);
SELECT * FROM SUPPLIER_PERFORMANCE_MV;


-- Q9 Identify the top 5 customers with the highest total sales in 2019, considering the number of unique products they purchased.
SELECT 
    TF.CustomerID,
    CD.CustomerName,
    COUNT(DISTINCT TF.ProductID) AS UniqueProductsPurchased,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN CUSTOMER_DIM CD ON TF.CustomerID = CD.CustomerID
    INNER JOIN TIME_DIM TD ON TF.OrderDate = TD.Date_
WHERE YEAR(TD.Date_) = 2019
GROUP BY TF.CustomerID, CD.CustomerName
ORDER BY TotalSales DESC LIMIT 5;

-- Q10 Create a materialized view named "CUSTOMER_STORE_SALES_MV" that presents the
-- monthly sales analysis for each store and then customers wise.
DROP VIEW IF EXISTS CUSTOMER_STORE_SALES_MV;
CREATE VIEW CUSTOMER_STORE_SALES_MV AS
SELECT 
    TF.StoreID,
    SD.StoreName,
    CD.CustomerName,
    MONTH(TF.OrderDate) AS Month,
    SUM(TF.Sales) AS TotalSales
FROM 
    Transactions_Fact TF
    INNER JOIN STORE_DIM SD ON TF.StoreID = SD.StoreID
    INNER JOIN CUSTOMER_DIM CD ON TF.CustomerID = CD.CustomerID
GROUP BY TF.StoreID, SD.StoreName, TF.CustomerID, CD.CustomerName, MONTH(TF.OrderDate), YEAR(TF.OrderDate)
ORDER BY TF.StoreID, TF.CustomerID, MONTH(TF.OrderDate), YEAR(TF.OrderDate);
SELECT * FROM CUSTOMER_STORE_SALES_MV;