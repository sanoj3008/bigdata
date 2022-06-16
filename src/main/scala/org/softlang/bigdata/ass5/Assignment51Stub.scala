package org.softlang.bigdata.ass5

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

object Assignment51Stub {
    def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  implicit class BetterSeq[K, V](val xs: Seq[(K, V)]) {
    def reduceByKey(op: (V, V) => V): Map[K, V] = xs.groupMapReduce(x => x._1)(x => x._2)(op)
  }

  def main(args: Array[String]): Unit = {

    // Setup spark.
    val spark = SparkSession
      .builder()
      .master("local[8]") // Local Spark instance with 8 cores.
      .appName("Simple Application")
      .getOrCreate()

    val sc = spark.sparkContext


    // If you need your own partitioner, use this:
    //    val partitioner = new Partitioner {
    //
    //      override def numPartitions: Int = 8
    //
    //      override def getPartition(key: Any): Int = nonNegativeMod(key.hashCode(), numPartitions)
    //    }

    val partitioner = new Partitioner {

      override def numPartitions: Int = 2

      override def getPartition(key: Any): Int = key match {
        case -1 => 0
        case 1 => 1
      }
    }

    myFilter(sc.parallelize(0 to 9))(elem => elem % 2 == 0)
      .collect().foreach(e => println(e))

    myMap(sc.parallelize(0 to 9))(elem => elem * 2)
      .collect().foreach(e => println(e))

    println(myReduce(sc.parallelize(Seq("A","B","C","D","E")))(_+_))
  }

  // TODO: Implement the following methods only using the RDD methods for processing partitions (see assignment PDF).

  def myFilter[A: ClassTag](xs: RDD[A])(f: A => Boolean): RDD[A] = xs.mapPartitions(iterator => {
    iterator.filter(f)
  })

  def myMap[A: ClassTag, B: ClassTag](xs: RDD[A])(f: A => B): RDD[B] = xs.mapPartitions(iterator => {
    iterator.map(f)
  })

  def myReduce[A: ClassTag](xs: RDD[A])(op: (A, A) => A): A = xs.mapPartitions(iterator => {
    if(iterator.nonEmpty) {
      val head: A = iterator.drop(0).next()
      Iterator(iterator.foldLeft(head)(op))
    } else Iterator()
  }).collect().reduce(op)

  def myReduceByKey[K: ClassTag, V: ClassTag](xs: RDD[(K, V)])(op: (V, V) => V): RDD[(K, V)] = ???

  def myJoin[K: ClassTag, V1: ClassTag, V2: ClassTag](ls: RDD[(K, V1)], rs: RDD[(K, V2)]): RDD[(K, (V1, V2))] = ???
}
