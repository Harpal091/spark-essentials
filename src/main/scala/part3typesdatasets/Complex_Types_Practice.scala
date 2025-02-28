package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Complex_Types_Practice extends App {

  val spark = SparkSession.builder
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()


  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  val moviesDF = spark.read.format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

//Dates

  moviesDF.select(col("Title"), to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release"))/365)

//  date_add and date_sub


  /**
   * Exercise
   * 1. How do we deal with multiple date formats?
   * 2. Read the stocks DF and parse the dates
   */


  /**
   * Get the dates pares them then get the null values and again parse them , then again do it.
   */

  val moviesDateFormatedDF=moviesDF.select(col("Title"), col("Release_Date"),to_date(col("Release_Date"),"dd-MMM-yy").as("Actual_Release"))
  val moviesDateDifferentDF= moviesDateFormatedDF.select(col("Title"),col("Release_Date")).where(col("Actual_Release").isNull).
    select(col("Title"), col("Release_Date"), to_date(col("Release_Date"), "yyyy-MM-dd").as("Actual_Release"))

  val moviesDateDifferentDF2=moviesDateDifferentDF.select(col("Title"), col("Release_Date")).where(col("Actual_Release").isNull)
    .select(col("Title"), col("Release_Date"), to_date(col("Release_Date"), "MMMM, yyyy").as("Actual_Release"))
  moviesDateDifferentDF2

//  Rest Records have actuallu NULL release dates-> We can union them Maybe use a case statements as well.

  val stocksDF = spark.read.format("csv").option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  stocksDF.select(col("symbol"), to_date(col("date"), "MMM dd yyyy").alias("Formatted_Date"), col("price"))
//  stocksDF.show
//  stocksDF.printSchema()


//  Structures
//Version 1
  moviesDF.select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross") ).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))
//Version 2 ---> Expressions
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

//  Arrays

  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) //Array of Strings - split by  space or comma
  moviesWithWords.select(
    col("Title"),
    expr("Title_Worlds[0"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()



}

