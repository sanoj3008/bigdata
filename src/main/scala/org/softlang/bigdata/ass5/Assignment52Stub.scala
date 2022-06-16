package org.softlang.bigdata.ass5

import org.apache.spark.sql.{DataFrame, SparkSession}

object Assignment52Stub {

  def main(args: Array[String]): Unit = {

    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Needed to access the relevant spark SQL methods.
    import org.apache.spark.sql.functions._

    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("res/ass5/movies.csv")
      .withColumnRenamed("original_language", "country") // renaming original_language to the more simpler and shorter word country

    // TODO: Run your queries here.
    val LIMIT: Int = 100 // set a limit for the amount of shown rows

    // groups all records by the origin country and counts the grouped rows
    val movieCount = df.groupBy("country")
      .count()
      .withColumnRenamed("count", "movieCount")
      .sort(desc("movieCount"))

    // groups all records by the genres and origin country and counts the grouped rows.
    // this is just a step which can be further used in multiple queries.
    val genreCountryCount = df.groupBy("genres", "country")
      .count()
      .withColumnRenamed("count", "genresMovieCount")
      .filter(col("genres") =!= "null")

    // joins the two calculated data frames by the country column.
    val gcSpecific = genreCountryCount.join(movieCount.limit(10), "country")
      .sort(asc("genres"))

    // column row presentation to show relation between countries and genres.
    // because the requested count is already within the df (cmp. gcSpecific), we needn't to calculate something and just take the first value.
    // All invalid genre/country constellations are marked as not available (-1)
    val gcSpecificCount = gcSpecific.groupBy("genres")
      .pivot("country")
      .agg(first("genresMovieCount"))
      .na.fill(-1)

    // calculates the proportion of genre specific movies per country.
    // Therefor we divide the genreMovieCount by the total movieCount for each country and convert it to percentage with two digits.
    // In some cases the proportion is less then 0.01% and we receive a 0.0 as result even though there exist movies for this genre/country constellation.
    // The rest is done like in the previous step.
    val gcSpecificFraction = gcSpecific.withColumn("fraction", round((col("genresMovieCount") / col("movieCount"))*100,2))
      .groupBy("genres")
      .pivot("country")
      .agg(first("fraction"))
      .na.fill(-1)

    // execution of exercise two
    // a.) For exercise a we only need to present the movieCount data frame.
    movieCount.show(LIMIT, truncate = false)

    // b.)
    genreCountryCount.groupBy("genres")
      .sum("genresMovieCount")
      .withColumnRenamed("sum(genresMovieCount)", "genresCount")
      .sort(desc("genresCount"))
      .show(LIMIT, truncate = false)

    // c.) For exercise c we only need to present the gcSpecificCount data frame.
    gcSpecificCount.show(LIMIT, truncate = false)

    // d.) For exercise d we only need to present the gcSpecificFraction data frame.
    gcSpecificFraction.show(LIMIT, truncate = false)
  }


}
