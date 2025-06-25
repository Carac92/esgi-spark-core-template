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

  // Fonction pour calculer le V de Cramér (mesure d'association pour variables catégorielles)
  private def cramersV(df: DataFrame, col1: String, col2: String): Double = {
    val contingencyTable = df.groupBy(col1, col2).count()
    val n = df.count().toDouble

    // Calcul du chi-carré
    val observed = contingencyTable.collect()
    val totalByCol1 = df.groupBy(col1).count().collect().map(row => (row.getString(0), row.getLong(1))).toMap
    val totalByCol2 = df.groupBy(col2).count().collect().map(row => (row.getString(0), row.getLong(1))).toMap

    var chiSquare = 0.0
    observed.foreach { row =>
      val val1 = row.getString(0)
      val val2 = row.getString(1)
      val observedCount = row.getLong(2).toDouble
      val expectedCount = (totalByCol1(val1) * totalByCol2(val2)) / n

      if (expectedCount > 0) {
        chiSquare += math.pow(observedCount - expectedCount, 2) / expectedCount
      }
    }

    // Calcul du V de Cramér
    val distinctCol1 = df.select(col1).distinct().count()
    val distinctCol2 = df.select(col2).distinct().count()
    val minDim = math.min(distinctCol1 - 1, distinctCol2 - 1)

    if (minDim > 0) math.sqrt(chiSquare / (n * minDim)) else 0.0
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

      var cleanedDf = if (colsWithMissing.nonEmpty) {
        val condition = colsWithMissing.map(c => col(c).isNotNull && !col(c).equalTo("")).reduce(_ && _)
        df.filter(condition)
      } else df

      println(s"Nombre de lignes avant nettoyage : ${df.count()}")
      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")


      cleanedDf.describe().show()

      //Valeurs dupliquées
      println(s"Nombre de lignes avant nettoyage : ${cleanedDf.count()}")
      cleanedDf.dropDuplicates()

      println(s"Nombre de lignes après nettoyage : ${cleanedDf.count()}")

      //Remove score not between 0 and 100
      cleanedDf = cleanedDf.filter(col("Exam_Score").between(0, 100))
      cleanedDf.show()

      // Analyse de corrélation pour variables qualitatives (avant encodage)
      println("\nAnalyse des associations entre variables qualitatives (Chi-carré et V de Cramér) :")

      val qualitativeCols = Seq(
        "Parental_Involvement", "Access_to_Resources", "Extracurricular_Activities",
        "Motivation_Level", "Internet_Access", "Family_Income", "Teacher_Quality",
        "School_Type", "Peer_Influence", "Learning_Disabilities",
        "Parental_Education_Level", "Distance_from_Home", "Gender"
      )

      // Calcul des associations entre variables qualitatives
      for {
        i <- qualitativeCols.indices
        j <- i + 1 until qualitativeCols.length
      } {
        val col1 = qualitativeCols(i)
        val col2 = qualitativeCols(j)

        try {
          val cramersVValue = cramersV(cleanedDf, col1, col2)
          println(f"$col1%-25s <-> $col2%-25s = $cramersVValue%.4f")
        } catch {
          case e: Exception =>
            println(f"$col1%-25s <-> $col2%-25s = Erreur de calcul")
        }
      }

      // Association entre variables qualitatives et Exam_Score (ANOVA)
      println("\n🔗 Association variables qualitatives vs Exam_Score (effet sur la performance) :")

      qualitativeCols.foreach { catCol =>
        try {
          val grouped = cleanedDf.groupBy(catCol)
            .agg(
              avg("Exam_Score").alias("mean_score"),
              stddev("Exam_Score").alias("std_score"),
              count("Exam_Score").alias("count")
            )
            .orderBy(catCol)

          println(s"\n📋 Impact de $catCol sur Exam_Score :")
          grouped.show(false)

          // Calcul simple de l'étendue des moyennes pour mesurer l'effet
          val scores = grouped.select("mean_score").collect().map(_.getDouble(0))
          if (scores.nonEmpty) {
            val range = scores.max - scores.min
            println(f"   Étendue des moyennes: $range%.2f points")
          }
        } catch {
          case e: Exception =>
            println(s"Erreur lors de l'analyse de $catCol: ${e.getMessage}")
        }
      }

      // Avant l'encodage
      println("Données avant encodage :")
      cleanedDf.select("Parental_Involvement", "School_Type", "Gender").show(5)

      // Encode categorical variables
      cleanedDf = encodeCategoricalVariables(cleanedDf)

      // Après l'encodage
      println("Données après encodage :")
      cleanedDf.select("Parental_Involvement", "School_Type", "Gender").show(5)

      println("Données après encodage des variables catégorielles :")
      cleanedDf.describe().show()

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

      val categoricalCols = cleanedDf2.columns.diff(numericCols)
      categoricalCols.foreach { colName =>
        println(s"\n🔹 Répartition des valeurs pour la colonne '$colName' :")
        cleanedDf2.groupBy(col(colName))
          .count()
          .orderBy(desc("count"))
          .show(100, truncate = false)
      }


      //correlation des variables numeriques:

      println("\n📈 Matrice de corrélation (Pearson) :")

      val numericColsWithExamScore = numericCols :+ "Exam_Score"

      for {
        i <- numericColsWithExamScore.indices
        j <- i + 1 until numericColsWithExamScore.length
      } {
        val col1 = numericColsWithExamScore(i)
        val col2 = numericColsWithExamScore(j)
        val corrValue = cleanedDf2.stat.corr(col1, col2)  // Pearson par défaut
        println(f"$col1%-20s <-> $col2%-20s = $corrValue%.4f")
      }

      //On enleve les variables peu pertinente (les categorielles ici)
      val finalDf = cleanedDf2.select(numericColsWithExamScore.map(col): _*)

      val filePath = "../data/clean_df.parquet" // chemin relatif dans le projet
      finalDf.coalesce(1) // Pour un seul fichier Parquet
        .write
        .mode("overwrite")
        .parquet(filePath)
      println(s"✅ DataFrame final écrit dans '$filePath' en format Parquet.")

      val filePathWithCategoricals = "../data/clean_df_with_categoricals.parquet" // chemin relatif dans le projet
      cleanedDf2.coalesce(1) // Pour un seul fichier Parquet
        .write
        .mode("overwrite")
        .parquet(filePathWithCategoricals)

      println(s"✅ DataFrame final écrit dans '$filePathWithCategoricals' en format Parquet.")
    } finally {
      //coales permet de changer le nombre de partitions pour un dataframe
      // Par exemple, coalesce(1) pour réduire à une seule partition
      spark.stop()
    }
  }
}