package org.esgi

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._

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

    val spark = SparkSession.builder()
      .appName("CsvToSpark")
      .master("local[*]")
      .getOrCreate()

    try {
      val df = readCsv(spark, csvPath)

      df.printSchema()
      println(s"Total number of entries: ${df.count()}")
      //df.show(20, false)
      df.describe().show()

      // Missing values count per column
      val missingValues = df.select(
        df.columns.map(c =>
          sum(when(col(c).isNull || col(c) === "", 1).otherwise(0)).alias(c)
        ): _*
      )

      missingValues.show(false)

      val missingCounts = missingValues.collect()(0).getValuesMap[Long](df.columns)
      val colsWithMissing = missingCounts.filter { case (_, count) => count > 0 }.keys.toSeq

      println("Colonnes avec valeurs manquantes : " + colsWithMissing.mkString(", "))

      val cleanedDf = if (colsWithMissing.nonEmpty) {
        val condition = colsWithMissing.map(c => col(c).isNotNull && !col(c).equalTo("")).reduce(_ && _)
        df.filter(condition)
      } else df

      println(s"Nombre de lignes avant nettoyage : ${df.count()}")
      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")

      // Maintenant tu peux continuer avec cleanedDf

      cleanedDf.describe().show()

      println(s"Nombre de lignes avant nettoyage : ${cleanedDf.count()}")
      cleanedDf.dropDuplicates()

      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")

      // Optional Parquet write
      maybeOut.foreach { outPath =>
        df.write.mode("overwrite").parquet(outPath)
        println(s"DataFrame written to $outPath in Parquet format.")
      }
    } finally {
      while (true) {}
      spark.stop()
    }
  }
}