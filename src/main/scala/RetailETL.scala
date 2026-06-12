import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RetailETL {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("java.library.path", "C:\\hadoop\\bin")
    //setup spark-session
    val spark = SparkSession.builder()
      .appName("Retail ETL")
      .master("local[*]")
      .getOrCreate()

    // 1. Read raw data
    val rawDF = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .csv("data/raw/online_retail.csv")

    // 2. Clean data
    val cleanedDF = rawDF
      .filter(col("CustomerID").isNotNull)
      .filter(col("Quantity") > 0)
      .dropDuplicates()

    // 3. create business columns
    val enrichedDF = cleanedDF
      .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
      .withColumn("InvoiceData", to_date(col("InvoiceDate")))

    // 4. Aggregation Layer (OLAP Style)
    val salesByCountry = enrichedDF
      .groupBy("Country")
      .agg( sum("TotalAmount").alias("TotalRevenue"),
        count("InvoiceNo").alias("TotalOrders"))

    // 5. Register temp views for SQL queries
    enrichedDF.createOrReplaceTempView("enriched")
    salesByCountry.createOrReplaceTempView("sales_by_country")

    // 6. Run SQL analytics queries
    println("\n=== Revenue by Country ===")
    spark.sql(
      """SELECT Country,
        |       ROUND(SUM(TotalAmount), 2) AS TotalRevenue,
        |       COUNT(DISTINCT InvoiceNo)  AS TotalOrders
        |FROM enriched
        |GROUP BY Country
        |ORDER BY TotalRevenue DESC""".stripMargin
    ).show(20)

    println("\n=== Top 10 Best-Selling Products ===")
    spark.sql(
      """SELECT StockCode,
        |       Description,
        |       SUM(Quantity)              AS TotalQuantitySold,
        |       ROUND(SUM(TotalAmount), 2) AS TotalRevenue
        |FROM enriched
        |GROUP BY StockCode, Description
        |ORDER BY TotalQuantitySold DESC
        |LIMIT 10""".stripMargin
    ).show(truncate = false)

    println("\n=== Top 10 Customers by Revenue ===")
    spark.sql(
      """SELECT CustomerID,
        |       COUNT(DISTINCT InvoiceNo)  AS TotalOrders,
        |       ROUND(SUM(TotalAmount), 2) AS TotalSpend
        |FROM enriched
        |GROUP BY CustomerID
        |ORDER BY TotalSpend DESC
        |LIMIT 10""".stripMargin
    ).show()

    println("\n=== Monthly Revenue Trend ===")
    spark.sql(
      """SELECT DATE_FORMAT(InvoiceData, 'yyyy-MM') AS Month,
        |       ROUND(SUM(TotalAmount), 2)          AS MonthlyRevenue,
        |       COUNT(DISTINCT InvoiceNo)           AS MonthlyOrders
        |FROM enriched
        |GROUP BY Month
        |ORDER BY Month""".stripMargin
    ).show(20)

    println("\n=== Average Order Value by Country ===")
    spark.sql(
      """SELECT Country,
        |       ROUND(SUM(TotalAmount) / COUNT(DISTINCT InvoiceNo), 2) AS AvgOrderValue,
        |       COUNT(DISTINCT CustomerID)                             AS UniqueCustomers
        |FROM enriched
        |GROUP BY Country
        |ORDER BY AvgOrderValue DESC""".stripMargin
    ).show(20)

    println("\n=== RFM Summary (Top 20 Customers) ===")
    spark.sql(
      """SELECT CustomerID,
        |       DATEDIFF(MAX(InvoiceData), MIN(InvoiceData)) AS TenureDays,
        |       COUNT(DISTINCT InvoiceNo)                    AS Frequency,
        |       ROUND(SUM(TotalAmount), 2)                   AS Monetary
        |FROM enriched
        |GROUP BY CustomerID
        |ORDER BY Monetary DESC
        |LIMIT 20""".stripMargin
    ).show()

    // 7. Write curated data to Parquet
    enrichedDF.write
      .mode("overwrite")
      .parquet("data/processed/enriched")

    salesByCountry.write
      .mode("overwrite")
      .parquet("data/processed/country_summary")

    println("\nETL Pipeline Completed Successfully!")
    spark.stop()
  }
}//☁️ STEP 6 — Upload to S3 (Next Phase)