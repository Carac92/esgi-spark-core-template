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

  private def encodeCategoricalVariables(df: DataFrame): DataFrame = {
    df
      // Parental_Involvement: Low=0, Medium=1, High=2
      .withColumn("Parental_Involvement",
        when(col("Parental_Involvement") === "Low", 0)
          .when(col("Parental_Involvement") === "Medium", 1)
          .when(col("Parental_Involvement") === "High", 2)
          .otherwise(col("Parental_Involvement").cast("integer")))

      // Access_to_Resources: Low=0, Medium=1, High=2
      .withColumn("Access_to_Resources",
        when(col("Access_to_Resources") === "Low", 0)
          .when(col("Access_to_Resources") === "Medium", 1)
          .when(col("Access_to_Resources") === "High", 2)
          .otherwise(col("Access_to_Resources").cast("integer")))

      // Extracurricular_Activities: No=0, Yes=1
      .withColumn("Extracurricular_Activities",
        when(col("Extracurricular_Activities") === "No", 0)
          .when(col("Extracurricular_Activities") === "Yes", 1)
          .otherwise(col("Extracurricular_Activities").cast("integer")))

      // Motivation_Level: Low=0, Medium=1, High=2
      .withColumn("Motivation_Level",
        when(col("Motivation_Level") === "Low", 0)
          .when(col("Motivation_Level") === "Medium", 1)
          .when(col("Motivation_Level") === "High", 2)
          .otherwise(col("Motivation_Level").cast("integer")))

      // Internet_Access: No=0, Yes=1
      .withColumn("Internet_Access",
        when(col("Internet_Access") === "No", 0)
          .when(col("Internet_Access") === "Yes", 1)
          .otherwise(col("Internet_Access").cast("integer")))

      // Family_Income: Low=0, Medium=1, High=2
      .withColumn("Family_Income",
        when(col("Family_Income") === "Low", 0)
          .when(col("Family_Income") === "Medium", 1)
          .when(col("Family_Income") === "High", 2)
          .otherwise(col("Family_Income").cast("integer")))

      // Teacher_Quality: Low=0, Medium=1, High=2
      .withColumn("Teacher_Quality",
        when(col("Teacher_Quality") === "Low", 0)
          .when(col("Teacher_Quality") === "Medium", 1)
          .when(col("Teacher_Quality") === "High", 2)
          .otherwise(col("Teacher_Quality").cast("integer")))

      // School_Type: Public=0, Private=1
      .withColumn("School_Type",
        when(col("School_Type") === "Public", 0)
          .when(col("School_Type") === "Private", 1)
          .otherwise(col("School_Type").cast("integer")))

      // Peer_Influence: Negative=0, Neutral=1, Positive=2
      .withColumn("Peer_Influence",
        when(col("Peer_Influence") === "Negative", 0)
          .when(col("Peer_Influence") === "Neutral", 1)
          .when(col("Peer_Influence") === "Positive", 2)
          .otherwise(col("Peer_Influence").cast("integer")))

      // Learning_Disabilities: No=0, Yes=1
      .withColumn("Learning_Disabilities",
        when(col("Learning_Disabilities") === "No", 0)
          .when(col("Learning_Disabilities") === "Yes", 1)
          .otherwise(col("Learning_Disabilities").cast("integer")))

      // Parental_Education_Level: High School=0, College=1, Postgraduate=2
      .withColumn("Parental_Education_Level",
        when(col("Parental_Education_Level") === "High School", 0)
          .when(col("Parental_Education_Level") === "College", 1)
          .when(col("Parental_Education_Level") === "Postgraduate", 2)
          .otherwise(col("Parental_Education_Level").cast("integer")))

      // Distance_from_Home: Near=0, Moderate=1, Far=2
      .withColumn("Distance_from_Home",
        when(col("Distance_from_Home") === "Near", 0)
          .when(col("Distance_from_Home") === "Moderate", 1)
          .when(col("Distance_from_Home") === "Far", 2)
          .otherwise(col("Distance_from_Home").cast("integer")))

      // Gender: Male=0, Female=1
      .withColumn("Gender",
        when(col("Gender") === "Male", 0)
          .when(col("Gender") === "Female", 1)
          .otherwise(col("Gender").cast("integer")))
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

      // Avant l'encodage
      println("Données avant encodage :")
      cleanedDf.select("Parental_Involvement", "School_Type", "Gender").show(5)

      // Encode categorical variables
      val encodedDf = encodeCategoricalVariables(cleanedDf)

      // Après l'encodage
      println("Données après encodage :")
      encodedDf.select("Parental_Involvement", "School_Type", "Gender").show(5)

      println("Données après encodage des variables catégorielles :")
      encodedDf.describe().show()

      //Outliers

      val numericCols = Seq(
        "Hours_Studied", "Attendance", "Sleep_Hours", "Previous_Scores",
        "Tutoring_Sessions", "Physical_Activity"
      )

      numericCols.foreach { colName =>
        val quantiles = cleanedDf.stat.approxQuantile(colName, Array(0.05, 0.95), 0.0)
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

      var cleanedDf2 = numericCols.foldLeft(cleanedDf) { (tempDf, colName) =>
        val Array(q1, q3) = tempDf.stat.approxQuantile(colName, Array(0.25, 0.75), 0.0)
        val iqr = q3 - q1
        val lower = q1 - 1.5 * iqr
        val upper = q3 + 1.5 * iqr
        tempDf.filter(col(colName) >= lower && col(colName) <= upper)
      }

      //Remove score not between 0 and 100
      cleanedDf2 = cleanedDf2.filter(col("Exam_Score").between(0, 100))
      cleanedDf2.show()


      val categoricalCols = cleanedDf2.columns.diff(numericCols)
      categoricalCols.foreach { colName =>
        println(s"\n🔹 Répartition des valeurs pour la colonne '$colName' :")
        cleanedDf2.groupBy(col(colName))
          .count()
          .orderBy(desc("count"))
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