package org.softlang.bigdata.ass4

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TasksRDD {

  def main(args: Array[String]): Unit = {

    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()
    val sc = spark.sparkContext

    // The following are simple test cases to check you implementation.
    // Task 1:
    assert(task1(sc.parallelize(Seq(1, 2, 3))).collect().toSeq == Seq("odd", "even", "odd"))

    // Task 2:
    assert(task2(sc.parallelize(Seq("ClassA.scala", "ClassB.java", "readme.txt"))).collect().toSeq == Seq("ClassB.java"))

    // Task 3:
    assert(task3(sc.parallelize(Seq(1, 2, 3))) == 1 * 2 * 3)

    // Task 4:
    assert(task4(sc.parallelize(Seq("a", "b", "c"))) == "abc")

    // Task 5:
    assert(task5(sc.parallelize(Seq("m1", "m2", "m3")), "m1"))
    assert(!task5(sc.parallelize(Seq("m1", "m2", "m3")), "m4"))

    // Task 6:
    assert(task6(sc.parallelize(Seq(("k1", "v1"), ("k1", "v2"), ("k2", "v1")))).collect().toMap == Map("k1" -> Seq("v1", "v2"), "k2" -> Seq("v1")))

    // Task 7:
    val l = sc.parallelize(Seq(("k1", "v1"), ("k2", "v2")))
    val r = sc.parallelize(Seq(("k2", "vv2"), ("k3", "vv2")))

    assert(task7(l, r).collect().toSeq == Seq(("k2", "v2", "vv2")))
  }

  /**
   * Return a list of strings "even" and "odd" for the input sequence of integers.
   */
  def task1(seq: RDD[Int]): RDD[String] =
    seq.map(e => {
      if (e%2 == 0) "even"
      else "odd"
    })

  /**
   * Filter a list of path names for ".java" files.
   */
  def task2(seq: RDD[String]): RDD[String] =
    seq
      .filter(e => e.takeRight(5) == ".java")

  /**
   * Form the product of all numbers.
   */
  def task3(seq: RDD[Int]): Int =
    seq
      .reduce(_*_)

  /**
   * Concat the individual strings of seq to a single string.
   */
  def task4(seq: RDD[String]): String =
    seq
      .sortBy(letter => letter)
      .collect()
      .reduce(_+_)

  /**
   * Check if x is a member (contained) in the sequence.
   */
  def task5(seq: RDD[String], x: String): Boolean =
    seq
      .map(e => e == x)
      .reduce((e1, e2) => e1 || e2)

  /**
   * Implement grouping by a key.
   */
  def task6(seq: RDD[(String, String)]): RDD[(String, Seq[String])] =
    seq
      .map(elem => (elem._1 -> Seq(elem._2)))
      .reduceByKey((v1, v2) => v1.concat(v2))

  /**
   * Join the two sequences by the key (first entry of the tuple). Produce a 3-tuple containing
   * the results (key, left, right).
   */
  def task7(l: RDD[(String, String)], r: RDD[(String, String)]): RDD[(String, String, String)] =
    task6(l.union(r))
      .filter(elem => elem._2.length == 2)
      .map(elem => (elem._1, elem._2.head, elem._2(1)))

}
