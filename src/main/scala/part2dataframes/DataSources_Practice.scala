package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources_Practice extends  App{
  val spark= SparkSession.builder()
    .appName("Data Sources and Format")
    .config("spark.master", "local")
    .getOrCreate()




  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

/*
 Reading a DF
 -format
 -schema (Optional) or inferSchema, true
 -zero or more options
 -mode - failFast/dropMalformed / permissive(default)

*/
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path","src/main/resources/data/cars.json")
    .load()

//    .load("src/main/resources/data/cars.json")

//  Anoter way -> Use OptionMap

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()


//  Writing DataFrames  extremely similar
  /*
  -format
  -save mode = overwrite, append, ignore, errorIfExists
  -path
  - Zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()
  carsDFWithOptionMap.show()

  // JSON flags
//  We also have timestamp formats for second level precision
//  If JSON in single quotes, we use that to parse it
//  Default value for compression is uncompressed
//  Can use .json as well instead of load and remove format
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")


// CSV Flags
  val stockSchema= StructType(
    Array(
      StructField("Symbol", StringType),
      StructField("date", DateType),
      StructField("Price", DoubleType)
    )
  )

  spark.read
    .format("csv")
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")


//Parquet Flags

  carsDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")


//  TextFiles
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()


  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()
  /**
   * Exercise: read the movies DF, then write it as
   * - tab-separated values file
   * - snappy Parquet
   * - table "public.movies" in the Postgres DB
   */


  val moviesDF=spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .mode("overwrite")
    .save("src/main/resources/data/movies_tab_sep.csv")

  moviesDF.write
    .format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("src/main/resources/data/movies_snappy.parquet")


  moviesDF.write
    .format("jdbc")
    .mode("overwrite")
    .option("url", url)
    .option("dbtable", "public.movies")
    .option("user", user)
    .option("password", password)
    .save()

}
