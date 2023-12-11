DROP SCHEMA IF EXISTS `ELECTRONICA-DW`;
CREATE SCHEMA `ELECTRONICA-DW`;
use `ELECTRONICA-DW`;
-- Creating the Product Dimension table
DROP TABLE IF EXISTS PRODUCT_DIM;
CREATE TABLE PRODUCT_DIM (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductPrice DECIMAL(10,2)
);

-- Creating the Supplier Dimension table
DROP TABLE IF EXISTS SUPPLIER_DIM;
CREATE TABLE SUPPLIER_DIM (
    SupplierID INT PRIMARY KEY,
	SupplierName VARCHAR(255)
);

-- Creating the Customer Dimension table
DROP TABLE IF EXISTS CUSTOMER_DIM;
CREATE TABLE CUSTOMER_DIM (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(10)
);

-- Creating the Store Dimension table
DROP TABLE IF EXISTS STORE_DIM;
CREATE TABLE STORE_DIM (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(255)
);

-- Time Dimension Table
DROP TABLE IF EXISTS TIME_DIM;
CREATE TABLE TIME_DIM (
    Date_ DATE PRIMARY KEY,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT
);

-- Creating the Transactions Fact table
CREATE TABLE Transactions_Fact (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    ProductID INT,
    CustomerID INT,
    SupplierID INT,
    StoreID INT,
    Sales INT,
    FOREIGN KEY (ProductID) REFERENCES PRODUCT_DIM(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES CUSTOMER_DIM(CustomerID),
	FOREIGN KEY (OrderDate) REFERENCES TIME_DIM(Date_),
	FOREIGN KEY (StoreID) REFERENCES STORE_DIM(StoreID),
	FOREIGN KEY (SupplierID) REFERENCES SUPPLIER_DIM(SupplierID)	
);
