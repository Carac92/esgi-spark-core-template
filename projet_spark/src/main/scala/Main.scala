package org.esgi

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"))
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
      df.show(20)
      df.describe().show()

      // Missing values count per column
      val missingValues = df.select(
        df.columns.map(c =>
          sum(when(col(c).isNull || col(c) === "" || col(c) === "NULL", 1).otherwise(0)).alias(c)
        ): _*
      )

      missingValues.show(false)

      val missingCounts = missingValues.collect()(0).getValuesMap[Long](df.columns)
      val colsWithMissing = missingCounts.filter { case (_, count) => count > 0 }.keys.toSeq


      //Valeurs manquantes
      println("Colonnes avec valeurs manquantes : " + colsWithMissing.mkString(", "))

      val cleanedDf = if (colsWithMissing.nonEmpty) {
        val condition = colsWithMissing.map(c => col(c).isNotNull && !col(c).equalTo("")).reduce(_ && _)
        df.filter(condition)
      } else df

      println(s"Nombre de lignes avant nettoyage : ${df.count()}")
      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")

      // Maintenant tu peux continuer avec cleanedDf

      cleanedDf.describe().show()


      //Valeurs dupliquées
      println(s"Nombre de lignes avant nettoyage : ${cleanedDf.count()}")
      cleanedDf.dropDuplicates()

      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")

      //Outliers

      val numericCols = Seq(
        "Hours_Studied", "Attendance", "Sleep_Hours", "Previous_Scores",
        "Tutoring_Sessions", "Physical_Activity", "Exam_Score"
      )

      numericCols.foreach { colName =>
        val quantiles = cleanedDf.stat.approxQuantile(colName, Array(0.25, 0.75), 0.0)
        if (quantiles.length == 2) {
          val Q1 = quantiles(0)
          val Q3 = quantiles(1)
          val IQR = Q3 - Q1
          val lowerBound = Q1 - 1.5 * IQR
          val upperBound = Q3 + 1.5 * IQR

          val totalCount = cleanedDf.filter(col(colName).isNotNull).count()
          val outlierCount = cleanedDf.filter(col(colName) < lowerBound || col(colName) > upperBound).count()
          val outlierPct = (outlierCount.toDouble / totalCount) * 100

          println(f"\n📊 Colonne: $colName")
          println(f"  - Q1 = $Q1%.2f, Q3 = $Q3%.2f, IQR = $IQR%.2f")
          println(f"  - Limites: [${lowerBound}%.2f, ${upperBound}%.2f]")
          println(f"  - Total non-nuls : $totalCount")
          println(f"  - Valeurs aberrantes : $outlierCount ($outlierPct%.2f%%)")
        }
      }

      val cleanedDf2 = numericCols.foldLeft(cleanedDf) { (tempDf, colName) =>
        val Array(q1, q3) = tempDf.stat.approxQuantile(colName, Array(0.25, 0.75), 0.0)
        val iqr = q3 - q1
        val lower = q1 - 1.5 * iqr
        val upper = q3 + 1.5 * iqr
        tempDf.filter(col(colName) >= lower && col(colName) <= upper)
      }

      cleanedDf2.show()

      val categoricalCols = cleanedDf2.columns.diff(numericCols)
      categoricalCols.foreach { colName =>
        println(s"\n🔹 Répartition des valeurs pour la colonne '$colName' :")
        cleanedDf2.groupBy(col(colName))
          .count()
          .orderBy(desc("count")) // tri décroissant par fréquence
          .show(100, truncate = false)
      }


      // Optional Parquet write
      maybeOut.foreach { outPath =>
        df.write.mode("overwrite").parquet(outPath)
        println(s"DataFrame written to $outPath in Parquet format.")
      }
    } finally {
      //coales permet de changer le nombre de partitions pour un dataframe
      // Par exemple, coalesce(1) pour réduire à une seule partition
      spark.stop()
    }
  }
}