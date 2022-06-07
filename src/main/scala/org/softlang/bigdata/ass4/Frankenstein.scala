package org.softlang.bigdata.ass4

import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Frankenstein {
  val PATH_TO_CSV: String = "res/ass4/wc.csv"

  def main(args: Array[String]): Unit = {
    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    // We start with plain Spark (sql will follow later).
    val sc = spark.sparkContext

    // To get rid of extensive tracking messages (optional)
    sc.setLogLevel("ERROR")

    // file writer to store the result.
    val fw = new FileWriter(PATH_TO_CSV)
    fw.write("tasks, time \n")

    // Our local data to start with.
    val input: RDD[String] = sc.textFile("./res/ass4/frank.txt")

    input
      .flatMap(text => text.split(" ")) // Transforms text into word array by dividing the given text at each white space.
      .map(text => text.replaceAll("[^a-zA-Z0-9]", "")) // Reqex check: Removes all unneeded characters in each word.
      .filter(word => word != "") // removes the empty word from the list.
      .map(word => (word, 1)) // at the beginning, each word has a count of one.
      .reduceByKey((cnt1, cnt2) => cnt1 + cnt2) // merges same words by summarize their counter
      .sortBy(pair => pair._2, ascending = false) // sorts the collection: Highest occurrence at the top
      .map(pair => pair._1 + "," + pair._2 + "\n") // converts each tuple to a csv valid string
      .collect()
      .foreach(e =>fw.write(e)) // each element gets serialized into the given csv file.


    fw.flush()
    fw.close()

    // Stop spark.
    spark.stop()
  }
}
