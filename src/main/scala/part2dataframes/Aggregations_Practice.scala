package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations_Practice extends App{

  val spark = SparkSession.builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

//  Counting -  All values except NUll
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF

  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")) //Count Rows Including Nulls

// Counting Distinct Values

  moviesDF.select(countDistinct(col("Major_Genre")))


//  Count Approximate - Will not scan entire DF row by row

  moviesDF.select(approx_count_distinct(col("Major_Genre")))


//  Min Max Functions

  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating) as Min_Rating")


//  Sum Function and Avg

  moviesDF.select(sum(col("US_Gross")))
  moviesDF.select(avg(col("US_Gross")))


//  data science

  moviesDF.select(
    mean(col("Rotten_tomatoes_Rating")),
    stddev(col("Rotten_tomatoes_Rating"))
  )



//  Grouping

  val countByGenres = moviesDF.groupBy(col("Major_Genre")).count()  //Includes Null
  countByGenres

  val avgIMDBByGenre = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  avgIMDBByGenre

  val aggregationDF= moviesDF.groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg(col("IMDB_Rating")).as("Avg_Rating")
    )
  aggregationDF


  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */


//  1
  moviesDF.printSchema()
  val profit= col("US_Gross") +  col("Worldwide_Gross")
  moviesDF.select(sum(profit))
//413129480065


//  2
  moviesDF.select(countDistinct(col("Director")))
//  550


//  3
  moviesDF.select(
    mean(col("US_Gross")).as("Gross_Rev"),
    stddev(col("US_Gross")).as("Std_Gross_Rev")
  ).show()

//4

  moviesDF.groupBy(col("Director")).agg(
    avg(col("IMDB_Rating")).as("Avg_IMDB_Rating"),
    avg(col("US_Gross")).as("Avg_US_Gross")
  ).orderBy(col("Avg_IMDB_Rating").desc_nulls_last)
    .show()
}
