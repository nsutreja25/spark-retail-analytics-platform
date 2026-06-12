-- ============================================================
-- Retail Analytics SQL Queries
-- Run against Spark temp views: enriched, sales_by_country
-- ============================================================

-- 1. Total revenue and order count by country (descending)
SELECT Country,
       ROUND(SUM(TotalAmount), 2) AS TotalRevenue,
       COUNT(DISTINCT InvoiceNo)  AS TotalOrders
FROM enriched
GROUP BY Country
ORDER BY TotalRevenue DESC;

-- 2. Top 10 best-selling products by quantity
SELECT StockCode,
       Description,
       SUM(Quantity)              AS TotalQuantitySold,
       ROUND(SUM(TotalAmount), 2) AS TotalRevenue
FROM enriched
GROUP BY StockCode, Description
ORDER BY TotalQuantitySold DESC
LIMIT 10;

-- 3. Top 10 customers by revenue
SELECT CustomerID,
       COUNT(DISTINCT InvoiceNo)  AS TotalOrders,
       ROUND(SUM(TotalAmount), 2) AS TotalSpend
FROM enriched
GROUP BY CustomerID
ORDER BY TotalSpend DESC
LIMIT 10;

-- 4. Monthly revenue trend
SELECT DATE_FORMAT(InvoiceData, 'yyyy-MM') AS Month,
       ROUND(SUM(TotalAmount), 2)          AS MonthlyRevenue,
       COUNT(DISTINCT InvoiceNo)           AS MonthlyOrders
FROM enriched
GROUP BY Month
ORDER BY Month;

-- 5. Average order value by country
SELECT Country,
       ROUND(SUM(TotalAmount) / COUNT(DISTINCT InvoiceNo), 2) AS AvgOrderValue,
       COUNT(DISTINCT CustomerID)                             AS UniqueCustomers
FROM enriched
GROUP BY Country
ORDER BY AvgOrderValue DESC;

-- 6. RFM (Recency, Frequency, Monetary) summary per customer
SELECT CustomerID,
       DATEDIFF(MAX(InvoiceData), MIN(InvoiceData))  AS TenureDays,
       COUNT(DISTINCT InvoiceNo)                     AS Frequency,
       ROUND(SUM(TotalAmount), 2)                    AS Monetary
FROM enriched
GROUP BY CustomerID
ORDER BY Monetary DESC;
