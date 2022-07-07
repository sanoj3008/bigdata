package org.softlang.bigdata.ass8

import scala.collection.mutable
import scala.io.Source

object MySteam {

  def main(args: Array[String]): Unit = {

    // Our stream coming from the frank.txt file.
    val source = Source.fromFile("res/ass4/frank.txt")
    for (paragraph <- source.getLines) {
      // Splitting the paragraph into words.
      val words = paragraph.replaceAll("[^A-Za-z0-9]", " ").split(" ")
      // Passing the words to the consumers of this stream.
      for(word <- words){
        if(word != "") {
          consume1(word)
          consume2(word)
          consume3(word)
          consume4(word)
          consume5(word)
          consume6(word)
        }
      }
    }

    println("Total count: " + totalCount)
    println("Amount of distinct words: " + distinctWordCount)
    println("Ten most occured words: " + occurrences.toSeq.sortWith(_._2 > _._2).take(10))
    println("Longest word in the text: " + longestWord)
    println("Most often appeared word in the text and its number of appearances: (" + wordAppearance._1 + "," + wordAppearance._2 + ")")
    println("Mean word length: " + ((mean *1000).round / 1000.toDouble))

  }

  // Example (Task 1) --------------------------------------
  // TAG: This is the final total count for consumer method 1 (stored in variable totalCount).
  var totalCount = 0

  def consume1(word: String): Unit = {
    // TODO: Compute and store the number of total words in the text.
    totalCount = totalCount + 1
  }

  /* TAG
    - distinctWords = stores all distinct words in a set. That's necessary to check if a word already occurred.
    - distinctWordCount = counts amount of distinct words
   */
  val distinctWords: mutable.Set[String] = mutable.Set[String]()
  var distinctWordCount = 0
  // Task 2 -----------------------------------------------
  def consume2(word: String): Unit = {
    // TODO: Compute the number of distinct words in the text.
    if(!distinctWords.contains(word)) {
      distinctWords.add(word)
      distinctWordCount += 1
    }
  }


  // TAG: This field stores the occurrences of each word in the text
  var occurrences: mutable.Map[String, Int] = mutable.Map[String, Int]()
  // Task 3 -----------------------------------------------
  def consume3(word: String): Unit = {
    // TODO: Compute and store the word-counts in the text.
    occurrences.update(word, occurrences.getOrElse(word, 0) + 1)
  }

  // TAG: This field contains the longest word in the text. At the beginning, the field is initialised with the empty string.
  var longestWord = ""
  // Task 4 -----------------------------------------------
  def consume4(word: String): Unit = {
    // TODO: Compute and store the longest word in the text.
    // update longestWord, if you found a new longer word.
    if(longestWord.length < word.length) longestWord = word
  }

  /* TAG: The most occurred word and the number of occurrences is stored in this tuple and will be updated.
    At the beginning, wordAppearance is initialised with the empty string and a occurrence of 0.
    If there exists a more often used word, the function will update the tuple with the new most occurred word.
   */
  var wordAppearance = ("", 0)
  // Task 5 -----------------------------------------------
  def consume5(word: String): Unit = {
    // TODO: Compute and store the word that appears most often.
    // make use of the occurrences map, to get the number of occurrences for the current word in the stream.
    val occ = occurrences.get(word)
    // Update wordAppearance if the retrieved value is greater than the stored value in the tupel
    if(occ.isDefined && wordAppearance._2 < occ.get) {
      wordAppearance = (word, occ.get)
    }
  }

  // TAG: mean represents to current mean length of the words.
  var mean = 0.0
  // Task 6 -----------------------------------------------
  def consume6(word: String): Unit = {
    // TODO: Compute and store the mean length of the words.
    /* the mean can be computed by use of the already calculated amount of total words in the text
      The calculation of the mean for n elements [m(n)] referring to the mean for n-1 [m(n-1)] elements can be done as follows:
        m(n) = ((m(n-1) * (n-1)) * L ) / n
      where L is the length of the current word.
     */
    mean = ((mean * (totalCount-1))+word.length) / totalCount
  }

}
