package org.softlang.bigdata.ass1

import java.io.FileWriter
import scala.collection.mutable
import scala.util.Random

object Assignment1 {

  /**
   * The following methods simulates fake "big data".
   * It draws a random word out of the list ("word_0", "word_1" ... "word_n").
   * The idx is taken as seed, to produce the same word for the same index when repeating the call.
   */
  def generator(idx: Long, n: Int): String = {
    // Initialize random with seed.
    val random = new Random(idx)

    // Produce a word with up to n different options.
    "word_" + random.nextInt(n)
  }

  /**
   * This is a small helper that takes two immutable maps and sums up the values for the corresponding keys.
   * You need it for the map-reduce solution.
   */
  def op(l: Map[String, Int], r: Map[String, Int]): Map[String, Int] = {
    (l.keySet ++ r.keySet).map(key =>
      (key, l.getOrElse(key, 0) + r.getOrElse(key, 0))
    ).toMap
  }

  def main(args: Array[String]): Unit = {
    val size = 100
    val n = 10
    val ourStream: Seq[String] = (0 to size).map(x => generator(x, n))

    // Hint: This is how to initialize and update an empty mutable Map (be aware of "import scala.collection.mutable"):
//    val ourMutableMap: mutable.Map[String, Int] = mutable.Map[String, Int]()
    // ourMutableMap.update("word_1",1)
    // ourMutableMap.update("word_2",4)

    // Hint: This is how to initialize and update an immutable Map:
    // var ourImmutableMap: Map[String, Int] = Map(("word_1",1))
    // ourImmutableMap = ourImmutableMap.updated("word_2",4)

    // Hint: This is basic code to dump to a csv file.
    // val fw = new FileWriter("output.csv")
    // for ((word, count) <- ourMap){
    //   fw.write(word + "," + count + "\n")
    // }
    // fw.flush()
    // fw.close()

    // Your solution ...

    // solution of word count problem with scala mutable map
    val ourMutableMap: mutable.Map[String, Int] = mutable.Map[String, Int]()
    ourStream.foreach(elem => {
      val value = ourMutableMap.getOrElse(elem, 0)
      ourMutableMap(elem) = value +1
    })

    // solution of word count problem with scala immutable map
    val ourImmutableMap: Map[String, Int] = ourStream.map(elem => Map(elem -> 1) ).reduce(op)


    println("Result of word count with mutable map:")
    println(ourMutableMap)

    println("\nResult of word count with immutable map:")
    println(ourImmutableMap)
  }

}
