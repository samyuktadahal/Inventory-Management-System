CREATE DATABASE InventoryManagementSystem;

use InventoryManagementSystem;

/* STEP 1 : DATABASE DESIGN SCHEMA */

-- Staging Tables(Raw Data)

CREATE TABLE Staging_Sales(
TransactionID INT,
ProductID INT,
SaleDate DATETIME,
Quantity INT,
Price DECIMAL(10,2),
StoreID INT,
CustomerID INT,
LoadDate DATETIME DEFAULT GETDATE()
);

/* 2. CORE TABLES (Cleaned Data) */

CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    SupplierID INT
);

CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    StoreID INT,
    SaleDate DATE,
    TotalAmount DECIMAL(10,2),
    Quantity INT,
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID)
);
/* 3. AUDIT TABLE (Track ETL Jobs)  */

CREATE TABLE ETL_Audit (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    JobName VARCHAR(100),
    StartTime DATETIME,
    EndTime DATETIME,
    Status VARCHAR(20),
    RowsAffected INT
);

/* 3. STORED PROCEDURE FOR AUTOMATION */

-- PROCEDURE TO LOAD DATA 

CREATE PROCEDURE usp_LoadSalesData
AS
BEGIN
     BEGIN TRY
	        INSERT INTO FactSales (ProductID, StoreID, SaleDate, TotalAmount, Quantity)
			SELECT ProductID, StoreID, CAST(SaleDate as DATE), Price * Quantity, Quantity
			FROM Staging_Sales
			WHERE ProductID IN ( SELECT ProductID FROM DimProduct);

-- Log Success 

INSERT INTO ETL_Audit(JobName, StartTime, EndTime, Status, RowsAffected)
VALUES('usp_LoadSalesData', GETDATE(), GETDATE(), 'Success', @@ROWCOUNT);

END TRY
BEGIN CATCH

-- Log Failure 

INSERT INTO ETL_Audit(JobName, StartTime, EndTime, Status, RowsAffected)
VALUES('usp_LoadSalesData', GETDATE(), GETDATE(), 'Failed', 0);
THROW;
END CATCH
END;

--PROCEDURE FOR MONTHLY AGGREGATION

CREATE PROCEDURE usp_GenerateMonthlyReport
AS
BEGIN
     SELECT YEAR(SaleDate) AS Year,
	 MONTH (SaleDate) AS Month,
	 SUM (TotalAmount) AS Revenue,
	 SUM (Quantity) AS TotalUnitsSold
FROM FactSales
GROUP BY YEAR(SaleDate), MONTH(SaleDate);
END;

/* 4. IMPLEMENT CHANGE DATA CAPTURE (CDC) */

-- Enable CDC on Critical Tables to track historical changes : 
EXEC sys.sp_cdc_enable_db;  --Enable CDC at database level
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
	@source_name = 'FactSales',
	@role_name = NULL;

-- Query Changes using :
SELECT * FROM cdc.dbo_FactSales_CT;

/* 5. SECURITY AND DATA MASKING */

-- First Creating DimCustomer Table::
CREATE TABLE DimCustomer(
CustomerID INT IDENTITY(1,1) PRIMARY KEY,
CustomerName VARCHAR(100),
Email VARCHAR(100)
);

-- Dynamic Data Masking ::

ALTER TABLE DimCustomer
ALTER COLUMN Email VARCHAR(100) MASKED WITH (FUNCTION = 'email()');

-- Grant Permissions ::

CREATE ROLE SalesAnalyst;
GRANT SELECT ON FactSales TO SalesAnalyst;

-- Staging table for raw product data
CREATE TABLE Staging_Products (
    ProductID INT,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    SupplierID INT,
    LoadDate DATETIME DEFAULT GETDATE()
);

CREATE TABLE DimStore (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(100),
    Location VARCHAR(100)
);

-- Insert sample sales data
INSERT INTO Staging_Sales (TransactionID, ProductID, SaleDate, Quantity, Price, StoreID, CustomerID)
VALUES 
    (1, 101, '2023-10-01', 2, 25.50, 1, 500),
    (2, 102, '2023-10-01', 1, 45.00, 1, 501),
    (3, 101, '2023-10-02', 3, 25.50, 2, 502),
    (4, 103, '2023-10-03', 1, 30.00, 1, 503),
    (5, 102, '2023-10-04', 2, 45.00, 3, 504);

-- Insert sample product data
INSERT INTO Staging_Products (ProductID, ProductName, Category, SupplierID)
VALUES 
    (101, 'Laptop', 'Electronics', 1),
    (102, 'Smartphone', 'Electronics', 1),
    (103, 'Desk Chair', 'Furniture', 2),
    (104, 'Coffee Mug', 'Kitchen', 3);

/* 6. ETL QUERIES (TRANSFROM & LOAD) */

-- Remove duplicates and load into DimProduct
INSERT INTO DimProduct(ProductID, ProductName, Category, SupplierID)
SELECT DISTINCT
      ProductID,
	  ProductName,
	  Category,
	  SupplierID
FROM Staging_Products;

-- Clean & Load Sales Data 
INSERT INTO FactSales(ProductID, StoreID, SaleDate, TotalAmount, Quantity)
SELECT 
     s.ProductID,
	 s.StoreID,
	   CAST(s.Saledate AS DATE) AS SaleDate,
	   s.Price * s.Quantity AS TotalAmount,
	   s.Quantity
     FROM Staging_Sales s
	 WHERE s.ProductID IN (SELECT ProductID FROM DimProduct); -- Ensure Referential Integrity

-- Log Success

INSERT INTO ETL_Audit(JobName, StartTime, EndTime, Status, RowsAffected)
VALUES('Sales ETL', GETDATE(), GETDATE(), 'Success', @@ROWCOUNT);

/* ERROR HANDLING (INVALID DATA) */
INSERT INTO ETL_Audit(JobName, StartTime, EndTime, Status, RowsAffected)
SELECT 
     'Sales ETL - Invalid Data',
	 GETDATE(),
	 GETDATE(),
	 'Failed',
	 COUNT(*)
FROM Staging_Sales
WHERE ProductID NOT IN (SELECT ProductID FROM DimProduct);

/* 7. DATA CLEANSING QUERY FOR PRODUCT DATA */

-- Clean and standarized product categories before loading to DimProduct
UPDATE Staging_Products
SET Category = 
    CASE 
	    WHEN Category IN ('Electronics', 'Electronic', 'Electronics') THEN 'Electronics'
		WHEN Category IN ('Furniture', 'Furn', 'Office Furniture') THEN 'Furniture'
		WHEN Category IN ('Kitchen', 'Kitchen', 'KitchenWare') THEN 'Kitchen'
		ELSE Category
    END
WHERE LoadDate = CAST(GETDATE() AS DATE);

/* 8. SLOWLY CHANGING DIMENSION (SCD) TYPE 2 HANDLING */

--Add SCD Type 2 Columns to DimProduct
ALTER TABLE DimProduct ADD
     EffectiveDate DATE DEFAULT GETDATE(),
	 ExpirationDate DATE NULL,
	 IsCurrent BIT DEFAULT 1;

-- Procedure to Handle Product Dimension Changes
CREATE PROCEDURE usp_UpdateDimProduct
AS
BEGIN
    -- Expire old records that have changed
    UPDATE dp
    SET dp.ExpirationDate = GETDATE(),
        dp.IsCurrent = 0
    FROM DimProduct dp
    JOIN Staging_Products sp ON dp.ProductID = sp.ProductID
    WHERE dp.IsCurrent = 1
      AND (
          dp.ProductName <> sp.ProductName    -- "<>" Not Equal to sign  
          OR dp.Category <> sp.Category 
          OR dp.SupplierID <> sp.SupplierID
      );
END;

-- Insert New Versions of Changed Products
INSERT INTO DimProduct(ProductID, ProductName, Category, SupplierID, EffectiveDate)
SELECT sp.ProductID, sp.ProductName, sp.Category, sp.SupplierID, GETDATE()
FROM Staging_Products sp
INNER JOIN DimProduct dp ON sp.ProductID = dp.ProductID
WHERE dp.IsCurrent = 0
AND (
      dp.ProductName <> sp.ProductName
	  OR dp.Category <> sp.Category
	  OR dp.SupplierID <> sp.SupplierID
    );

-- Insert completely New Products
INSERT INTO DimProduct(ProductID, ProductName, Category, SupplierID)
SELECT sp.ProductID, sp.ProductName, sp.Category, sp.SupplierID
FROM Staging_Products sp
LEFT JOIN DimProduct dp ON sp.ProductID = dp.ProductID
WHERE dp.ProductID IS NULL;

/* 9. INVENTORY LEVEL TRACKING ETL */

-- Create Inventory Fact Table
CREATE TABLE FactInventory(
InventoryKey INT IDENTITY(1,1) PRIMARY KEY,
ProductID INT,
StoreID INT,
Date DATE,
QuantityOnHand INT,
ReorderLevel INT,
FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
FOREIGN KEY (StoreID) REFERENCES DimStore(StoreID)
);

-- ETL to load inventory data

ALTER TABLE DimProduct ADD
ReorderLevel INT ;

CREATE TABLE Staging_Inventory (
    InventoryID INT IDENTITY(1,1) PRIMARY KEY, -- Auto-increment ID
    ProductID INT NOT NULL,
    StoreID INT NOT NULL,
    InventoryDate DATETIME NOT NULL,
    Quantity INT NOT NULL,
    LoadDate DATE DEFAULT CAST(GETDATE() AS DATE) -- Default today's date
);

-- ETL to load inventory data
INSERT INTO FactInventory (ProductID, StoreID, Date, QuantityOnHand, ReorderLevel)
SELECT 
    i.ProductID,
    i.StoreID,
    CAST(i.InventoryDate AS DATE),
    i.Quantity,
    p.ReorderLevel
FROM Staging_Inventory i
JOIN DimProduct p ON i.ProductID = p.ProductID
WHERE i.LoadDate = CAST(GETDATE() AS DATE);

/* 10. DATA QUALITY CHECK QUERIES */

-- Check for missing product references in Sales Table 
INSERT INTO ETL_Audit (JobName, StartTime, EndTime, Status, RowsAffected)
SELECT 
    'Data Quality - Missing Product References',
    GETDATE(),
    GETDATE(),
    'Warning',
    COUNT(*)
FROM Staging_Sales
WHERE ProductID NOT IN (SELECT ProductID FROM DimProduct);

-- Check for negative quantities for prices
INSERT INTO ETL_Audit(JobName, StartTime, EndTime, Status, RowsAffected)
SELECT 
      'Data Quality - Invalid Values',
	  GETDATE(),
	  GETDATE(),
	  CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Success' END,
	  COUNT(*)
FROM Staging_Sales
WHERE Quantity <= 0 OR Price <= 0;

/* 11. TIME DIMENSION ETL */

-- Create Time Dimension Table 
CREATE TABLE DimDate(
DateKey INT PRIMARY KEY,
Date DATE,
Day INT,
Month INT,
Year INT,
Quarter INT,
DayOfWeek INT,
DayName VARCHAR(10),
MonthName VARCHAR(10),
IsWeekend BIT
);

-- Populate time dimension (could be a stored procedure)
INSERT INTO DimDate
SELECT
      CONVERT(INT, CONVERT(VARCHAR, Date, 112)) AS DateKey,
	  Date,
	  DAY(Date) AS Day,
	  MONTH(Date) AS Month,
	  YEAR(Date) AS Year,
	  DATEPART(QUARTER, Date) AS Quarter,
	  DATEPART(WEEKDAY, Date) AS DayOfWeek,
	  DATENAME(WEEKDAY, Date) AS DayName,
	  DATENAME(MONTH, Date) AS MonthName,
	  CASE WHEN DATEPART(WEEKDAY, Date) IN (1,7) THEN 1 ELSE 0 END AS IsWeekend
FROM(
     SELECT DISTINCT CAST(SaleDate AS DATE) AS Date
	 FROM Staging_Sales
	 UNION
	 SELECT DISTINCT CAST(InventoryDate AS DATE)
	 FROM Staging_Inventory
	) AS Dates;

/* 12. SUPPLIER DIMENSION ETL */

-- Create Supplier Staging and Dimension Tables 
CREATE TABLE Staging_Suppliers(
SupplierID INT,
SupplierName VARCHAR(100),
ContactPerson VARCHAR(100),
Phone VARCHAR(20),
Email VARCHAR(100),
LoadDate DATETIME DEFAULT GETDATE()
);

CREATE TABLE DimSupplier(
SupplierID INT PRIMARY KEY,
SupplierName VARCHAR(100),
ContactPerson VARCHAR(100),
Phone VARCHAR(20),
Email VARCHAR(100),
EffectiveDate DATE,
ExpirationDate DATE NULL,
IsCurrent BIT DEFAULT 1
);

-- Supplier ETL Procedure
CREATE PROCEDURE usp_LoadSupplierData
AS
BEGIN
     -- Handle New Suppliers
	 INSERT INTO DimSupplier (SupplierID, SupplierName, ContactPerson, Phone, Email, EffectiveDate)
	 SELECT s.SupplierID, s.SupplierName, s.ContactPerson, s.Phone, s.Email, GETDATE()
	 FROM Staging_Suppliers s
	 LEFT JOIN DimSupplier d ON s.SupplierID = d.SupplierID
	 WHERE d.SupplierID IS NULL;
END;

-- Handle supplier updates (SCD Type 2)
-- Expire old records
UPDATE d
SET d.ExpirationDate = GETDATE(),
    d.IsCurrent = 0
FROM DimSupplier d
INNER JOIN Staging_Suppliers s ON d.SupplierID = s.SupplierID
WHERE d.IsCurrent = 1
AND (d.SupplierName <> s.SupplierName OR d.ContactPerson <> s.ContactPerson OR d.Phone <> s.Phone OR d.Email <> s.Email);

-- Insert New Versions

INSERT INTO DimSupplier(SupplierID, SupplierName, ContactPerson,Phone, Email, EffectiveDate)
SELECT s.SupplierID, s.SupplierName, s.ContactPerson, s.Phone, s.Email, GETDATE()
FROM Staging_Suppliers s
JOIN DimSupplier d ON  s.SupplierID = d.SupplierID
WHERE d.IsCurrent = 0 
AND (d.SupplierName <> s.SupplierName OR d.ContactPerson <> s.ContactPerson OR d.Phone <> s.Phone OR d.Email <> s.Email);

/* 13. SALES TREND ANALYSIS ETL */

-- Create Sales Trend Analysis Table
CREATE TABLE FactSalesTrend(
TrendKey INT IDENTITY(1,1) PRIMARY KEY,
ProductID INT,
Date DATE,
MovingAvg7Day DECIMAL(10,2),
MovingAvg30Day DECIMAL(10,2),
SalesGrowthRate DECIMAL(10,4),
FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID)
);


-- Calculate and store sales trends
INSERT INTO FactSalesTrend (ProductID, Date, MovingAvg7Day, MovingAvg30Day, SalesGrowthRate)

WITH DailySales AS (
    SELECT 
        ProductID,
        SaleDate AS Date,
        SUM(TotalAmount) AS DailySales
    FROM FactSales
    GROUP BY ProductID, SaleDate
),
SalesWithPrev AS (
    SELECT 
        ProductID,
        Date,
        DailySales,
        LAG(DailySales, 1) OVER (PARTITION BY ProductID ORDER BY Date) AS PrevDaySales,
        AVG(DailySales) OVER (PARTITION BY ProductID ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS MA7,
        AVG(DailySales) OVER (PARTITION BY ProductID ORDER BY Date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MA30
    FROM DailySales
)
INSERT INTO FactSalesTrend (ProductID, Date, MovingAvg7Day, MovingAvg30Day, SalesGrowthRate)
SELECT 
    ProductID,
    Date,
    MA7 AS MovingAvg7Day,
    MA30 AS MovingAvg30Day,
    CASE 
        WHEN PrevDaySales = 0 THEN NULL 
        ELSE (DailySales - PrevDaySales) / PrevDaySales 
    END AS SalesGrowthRate
FROM SalesWithPrev;

/* 14. INCREMENTAL LOAD PATTERN */

-- Load Only New or Changed Records From Staging to Fact Tables
INSERT INTO FactSales (ProductID, StoreID, SaleDate, TotalAmount, Quantity)
SELECT 
      s.ProductID,
	  s.StoreID,
	  CAST(s.SaleDate AS DATE),
	  (s.Price * s.Quantity) AS TotalSale,
	  s.Quantity
FROM Staging_Sales s 
LEFT JOIN FactSales f ON s.ProductID = f.ProductID
WHERE f.ProductID = NULL -- Only New Records
AND s.LoadDate > (SELECT MAX(EndTime) FROM ETL_Audit WHERE JobName = 'Sales ETL');

/* 15. DATA VALIDATION QUERIES */

-- Check for Foreign Key Violations (Check for Orphaned Records)
SELECT s.*
FROM Staging_Sales s
INNER JOIN DimProduct d ON s.ProductID = d.ProductID
WHERE d.ProductID = NULL;

--Validate the Data Completeness
SELECT 
      COUNT(CASE WHEN ProductName IS NULL THEN 1 END) AS MissingProductNames,
	  COUNT(CASE WHEN Category IS NULL THEN 1 END) AS MissingCategories,
	  COUNT (*) AS TotalRecords
FROM Staging_Products;

/* 16. TYPE 2 SCD(SLOWLY CHANGING DIMENSION) HANDLING */

-- Expire Current Records That Have Changed 
UPDATE DimProduct
SET ExpirationDate = GETDATE(),
    IsCurrent = 0
FROM DimProduct dp
INNER JOIN Staging_Products sp ON dp.ProductID = sp.ProductID
WHERE dp.IsCurrent = 1
AND (dp.ProductName <> sp.ProductName OR dp.SupplierID <> sp.SupplierID OR dp.Category <> sp.Category);

-- Insert New Versions of Changed Products

INSERT INTO DimProduct(ProductID, ProductName, Category, SupplierID, EffectiveDate)
SELECT 
      sp.ProductID,
	  sp.ProductName,
	  sp.Category,
	  sp.SupplierID,
	  GETDATE()
FROM Staging_Products sp
INNER JOIN DimProduct dp ON sp.ProductID = dp.ProductID
WHERE dp.IsCurrent = 0
AND (dp.ProductID <> sp.ProductID OR dp.ProductName <> sp.ProductName OR dp.Category <> sp.Category);

/* 17. DATA CLEANSING TRANSFORMATIONS */

-- Standarized Product Categories
UPDATE Staging_Products
SET Category = 
    CASE  
	    WHEN LOWER(Category) LIKE '%phone' THEN 'SmartPhone'
		WHEN LOWER(Category) LIKE '%lap' THEN 'Laptop'
		WHEN LOWER(Category) LIKE '%furn' THEN 'Furniture'
		ELSE Category
	END;

-- Clean Numeric Fields 
UPDATE Staging_Sales
SET 
   Quantity = ABS(Quantity),
   Price = ABS(Price)
   WHERE Quantity < 0 OR Price < 0;

/* 18. AGGREGATION AND SUMMARY LOADS */

-- Create the daily sales summary fact table
CREATE TABLE FactDailySalesSummary (
    SummaryID INT IDENTITY(1,1) PRIMARY KEY,
    Date DATE NOT NULL,
    ProductID INT NOT NULL,
    StoreID INT NOT NULL,
    TotalSales DECIMAL(12,2) NOT NULL,
    TotalQuantity INT NOT NULL,
    AverageUnitPrice DECIMAL(10,2) NOT NULL,
    DayOfWeek VARCHAR(10) NOT NULL,
    IsWeekend BIT NOT NULL,
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID)
);

-- Create index for better query performance
CREATE INDEX IX_FactDailySalesSummary_Date ON FactDailySalesSummary(Date);
CREATE INDEX IX_FactDailySalesSummary_ProductID ON FactDailySalesSummary(ProductID);
CREATE INDEX IX_FactDailySalesSummary_StoreID ON FactDailySalesSummary(StoreID);

-- Create daily sales summary
INSERT INTO FactDailySalesSummary (Date, ProductID, TotalSales, TotalQuantity)
SELECT 
    CAST(SaleDate AS DATE),
    ProductID,
    SUM(TotalAmount),
    SUM(Quantity)
FROM FactSales
WHERE SaleDate >= DATEADD(day, -1, GETDATE())
GROUP BY CAST(SaleDate AS DATE), ProductID;

/* 19. DATA QUALITY METRICES */

-- Create the Data Quality Metrics table
CREATE TABLE DataQuality_Metrics (
    MetricID INT IDENTITY(1,1) PRIMARY KEY,
    MetricDate DATETIME NOT NULL DEFAULT GETDATE(),
    MetricCategory VARCHAR(50) NOT NULL,
    MetricName VARCHAR(100) NOT NULL,
    MetricValue DECIMAL(10,4) NOT NULL,
    ThresholdValue DECIMAL(10,4) NULL,
    Status AS CASE 
                WHEN ThresholdValue IS NULL THEN 'N/A'
                WHEN MetricValue >= ThresholdValue THEN 'Passed' 
                ELSE 'Failed' 
             END,
    SourceSystem VARCHAR(50) NOT NULL,
    TableName VARCHAR(128) NULL,
    ColumnName VARCHAR(128) NULL,
    CheckDescription VARCHAR(255) NULL,
    RecordsTested INT NULL,
    InvalidRecords INT NULL
);

-- Create indexes for better performance
CREATE INDEX IX_DataQuality_Metrics_Date ON DataQuality_Metrics(MetricDate);
CREATE INDEX IX_DataQuality_Metrics_Category ON DataQuality_Metrics(MetricCategory);
CREATE INDEX IX_DataQuality_Metrics_Status ON DataQuality_Metrics(Status);

-- Insert sample data quality metrics
INSERT INTO DataQuality_Metrics (
    MetricCategory,
    MetricName,
    MetricValue,
    ThresholdValue,
    SourceSystem,
    TableName,
    ColumnName,
    CheckDescription,
    RecordsTested,
    InvalidRecords
)
VALUES
-- Completeness metrics
(
    'Completeness',
    'Product Name Completeness',
    99.82,
    99.50,
    'ProductCatalog',
    'Staging_Products',
    'ProductName',
    'Percentage of non-NULL ProductName values',
    12547,
    23
),
(
    'Completeness',
    'Sales Quantity Completeness',
    100.00,
    99.90,
    'POS_System',
    'Staging_Sales',
    'Quantity',
    'Percentage of non-NULL Quantity values',
    458721,
    0
),

-- Validity metrics
(
    'Validity',
    'Valid ProductID References',
    98.67,
    99.00,
    'POS_System',
    'Staging_Sales',
    'ProductID',
    'Percentage of ProductIDs that exist in DimProduct',
    458721,
    6124
),
(
    'Validity',
    'Positive Sales Quantities',
    99.95,
    99.80,
    'POS_System',
    'Staging_Sales',
    'Quantity',
    'Percentage of Quantity values > 0',
    458721,
    225
),

-- Uniqueness metrics
(
    'Uniqueness',
    'Unique Product Codes',
    100.00,
    100.00,
    'ProductCatalog',
    'DimProduct',
    'ProductCode',
    'Percentage of unique ProductCode values',
    12458,
    0
),
(
    'Uniqueness',
    'Duplicate Customer Records',
    0.15,
    0.50,
    'CRM_System',
    'Staging_Customers',
    'Email',
    'Percentage of duplicate email addresses',
    87542,
    132
),

-- Consistency metrics
(
    'Consistency',
    'Price Consistency',
    99.40,
    99.00,
    'POS_System',
    'FactSales',
    'UnitPrice',
    'Percentage of prices matching product catalog',
    458721,
    2752
),
(
    'Consistency',
    'Date Range Consistency',
    100.00,
    100.00,
    'POS_System',
    'Staging_Sales',
    'SaleDate',
    'Percentage of dates within valid business period',
    458721,
    0
),

-- Timeliness metrics
(
    'Timeliness',
    'On-Time Daily Loads',
    95.50,
    98.00,
    'ETL_System',
    NULL,
    NULL,
    'Percentage of daily loads completed by 6:00 AM',
    30,
    1
),
(
    'Timeliness',
    'Data Freshness',
    99.90,
    99.50,
    'POS_System',
    'Staging_Sales',
    'SaleDate',
    'Percentage of records loaded within 1 hour of transaction',
    458721,
    458
),

-- Accuracy metrics (requires reference data)
(
    'Accuracy',
    'Customer Address Accuracy',
    92.30,
    90.00,
    'CRM_System',
    'DimCustomer',
    'PostalCode',
    'Percentage matching postal code validation service',
    87542,
    6754
),
(
    'Accuracy',
    'Product Category Accuracy',
    97.80,
    95.00,
    'ProductCatalog',
    'DimProduct',
    'Category',
    'Percentage matching master category list',
    12458,
    277
);

-- Calculate Data Quality Metrices
INSERT INTO DataQuality_Metrics(MetricDate, MetricName, MetricCategory, MetricValue)
SELECT 
      GETDATE(),
	  'Product Completeness',
	  'Completeness',
	  COUNT(CASE WHEN ProductName IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)
FROM Staging_Products 

UNION

SELECT
       GETDATE(),
	  'Sales Validity',
	  'Validity',
	   COUNT(CASE WHEN Quantity > 0 AND Price > 0 THEN 1 END) * 100.0 / COUNT(*)
FROM Staging_Sales;

/* 20. DATA QUALITY DASHBOARD QUERY */ 

SELECT 
    MetricDate,
    MetricCategory,
    MetricName,
    MetricValue,
    ThresholdValue,
    Status,
    SourceSystem,
    TableName,
    ColumnName,
	CheckDescription,
    RecordsTested,
    InvalidRecords,
    CAST(InvalidRecords * 100.0 / NULLIF(RecordsTested, 0) AS DECIMAL(5,2)) AS ErrorPercentage
FROM DataQuality_Metrics
WHERE MetricDate >= DATEADD(DAY, -7, GETDATE())
ORDER BY 
    MetricCategory,
    CASE Status WHEN 'Failed' THEN 0 
	ELSE 1 
	END,
    MetricDate DESC;



