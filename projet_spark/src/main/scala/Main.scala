package org.esgi

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {

  private def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")        // Use first row as header
      .option("inferSchema", "true")    // Automatically infer column types
      .option("mode", "DROPMALFORMED")  // Drop malformed rows instead of failing
      .csv(path)
  }

  def main(args: Array[String]): Unit = {
    // Default relative path inside the repo
    val defaultCsv = "data/StudentPerformanceFactors.csv"

    // Determine paths from CLI or fallback
    val csvPath   = args.headOption.getOrElse(defaultCsv)
    val maybeOut  = if (args.length > 1) Some(args(1)) else None

    // Boilerplate SparkSession creation; master/driver memory come from spark‑submit
    val spark = SparkSession
      .builder()
      .appName("CsvToSpark")
      .getOrCreate()

    try {
      val df = readCsv(spark, csvPath)

      // Quick inspection
      println(s"\n=== Loaded file: $csvPath ===")
      df.printSchema()
      df.show(numRows = 20, truncate = false)

      // Optional Parquet write
      maybeOut.foreach { outPath =>
        df.write.mode("overwrite").parquet(outPath)
        println(s"\nDataFrame written to $outPath in Parquet format.")
      }
    } finally {
      spark.stop()
    }
  }
}