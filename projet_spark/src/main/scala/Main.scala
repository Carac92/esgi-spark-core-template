package org.esgi

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames

object Main {

  private def readCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true") // Use first row as header
      .option("inferSchema", "true") // Automatically infer column types
      .option("mode", "DROPMALFORMED") // Drop malformed rows instead of failing
      .csv(path)
  }

  def main(args: Array[String]): Unit = {
    // Default relative path inside the repo
    val defaultCsv = "data/StudentPerformanceFactors.csv"

    // Determine paths from CLI or fallback
    val csvPath = args.headOption.getOrElse(defaultCsv)
    val maybeOut = if (args.length > 1) Some(args(1)) else None

    // Boilerplate SparkSession creation; master/driver memory come from spark‑submit
    val spark = SparkSession
      .builder()
      .appName("CsvToSpark")
      .master(
        "local[*]"
      ) // ← ici : exécution en local avec tous les cœurs disponibles
      .getOrCreate()

    try {
      var df = readCsv(spark, csvPath)

      // Quick inspection
      println(s"\n=== Loaded file: $csvPath ===")
      df.printSchema()
      df.show(numRows = 20, truncate = false)
      var a = df.describe()

      // Optional Parquet write
      maybeOut.foreach { outPath =>
        df.write.mode("overwrite").parquet(outPath)
        println(s"\nDataFrame written to $outPath in Parquet format.")
      }
    } finally {
      while (true) {}
      spark.stop()
    }
  }
}
