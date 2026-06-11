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

    //5. Write curated data Parquet
    enrichedDF.write
      .mode("overwrite")
      .parquet("data/processed/country_summary")

    println("ETL Pipeline Completed Successfully!")
    spark.stop()
  }
}//☁️ STEP 6 — Upload to S3 (Next Phase)