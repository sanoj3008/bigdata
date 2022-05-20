package org.softlang.bigdata.ass3

object Tasks {

  def main(args: Array[String]): Unit = {

    // The following are simple test cases to check you implementation.
    // Task 1:
    assert(task1(Seq(1, 2, 3)) == Seq("odd", "even", "odd"))

    // Task 2:
    assert(task2(Seq("ClassA.scala", "ClassB.java", "readme.txt")) == Seq("ClassB.java"))

    // Task 3:
    assert(task3(Seq(1, 2, 3)) == 1 * 2 * 3)

    // Task 4:
    assert(task4(Seq("a", "b", "c")) == "abc")

    // Task 5:
    assert(task5(Seq("m1", "m2", "m3"), "m1"))
    assert(!task5(Seq("m1", "m2", "m3"), "m4"))

    // Task 6:
    assert(task6(Seq(("k1", "v1"), ("k1", "v2"), ("k2", "v1"))) == Map("k1" -> Seq("v1", "v2"), "k2" -> Seq("v1")))

    // Task 7:
    val l = Seq(("k1", "v1"), ("k2", "v2"))
    val r = Seq(("k2", "vv2"), ("k3", "vv2"))
    assert(task7(l, r) == Seq(("k2", "v2", "vv2")))
  }

  /**
   * Return a list of strings "even" and "odd" for the input sequence of integers.
   */
  def task1(seq: Seq[Int]): Seq[String] = seq.map(e => {
    if (e%2 == 0) "even"
     else "odd"
  })

  /**
   * Filter a list of path names for ".java" files.
   */
  def task2(seq: Seq[String]): Seq[String] = seq.filter(e => e.takeRight(5) == ".java")

  /**
   * Form the product of all numbers contained in the collection.
   */
  def task3(seq: Seq[Int]): Int = seq.reduce((x,y) => x*y)
  def task3Improved(seq: Seq[Int]): Int = seq.product

  /**
   * Concat the individual strings of this collection forming a single string.
   */
  def task4(seq: Seq[String]): String = seq.reduce((a,b) => a+b)

  /**
   * Check if x is a member (contained) in the sequence.
   */
  def task5(seq: Seq[String], x: String): Boolean = seq.map(e => e == x).reduce((x,y) => x || y)
  def task5Improved(seq: Seq[String], x: String): Boolean = seq.contains(x)

  /**
   * (Optional) Implement grouping by a key, without using the "groupBy" method provided on the collection,
   * but the fold method.
   * The first entry of the tuples is the key that you should use for grouping.
   * Optional: This is an advanced assignment, you may get additional points.
   */
  def task6(seq: Seq[(String, String)]): Map[String, Seq[String]] = seq.map(elem => Map(elem._1 -> Seq(elem._2)))
    .fold(Map.empty[String, Seq[String]])((result, elem) => {
      val (key, value) = elem.head
      result.updated(key, result.getOrElse(key, Seq()).appended(value.head))
    })

  /**
   * (Optional) Join the two sequences by the key (first entry of the tuple). Produce a 3-tuple containing
   * the results (key, left, right).
   * Optional: This is an advanced assignment, you may get additional points.
   */
  def task7(l: Seq[(String, String)], r: Seq[(String, String)]): Seq[(String, String, String)] = task6(l.concat(r))
    .filter(elem => elem._2.length == 2)
    .toSeq
    .map(elem => (elem._1, elem._2.head, elem._2(1)))
}
