package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes_Practice extends  App {
  val spark = SparkSession.builder()
    .appName("Common Spark Type")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF= spark.read
     .format("json")
     .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

//  moviesDF.show()
// Adding a plain value to a DF

  moviesDF.select(col("Title"), lit(43).alias("PlainValue")).show()

//  Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select(col("Title")).where(dramaFilter)

//  Multiple ways of filtering

  val moviesWithGoodnessFlags = moviesDF.select(col("Title"), preferredFilter.as("GoodMovie"))
//  Filter on column Name
  moviesWithGoodnessFlags.where("GoodMovie")

  moviesWithGoodnessFlags.where(not(col("GoodMovie"))).show()


  //Numbers

  val moviesAvgRatingsDF= moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating")/10  + col("IMDB_Rating"))/2)

//  Correlation = number between -1 _and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))  /*Correlation is an Action */

//  Strings

  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
//Capitalize  -> lower/upper
  carsDF.select(initcap(col("Name")))

// Contains
  carsDF.select("*").where(col("Name").contains("volkswagen")).show()


//  regex

  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )




}
