package org.softlang.bigdata.ass7

//import org.softlang.bigdata.ass7.MyDistributedKVStore2.random

import scala.collection.mutable
import scala.util.Random

object MyDistributedKVStore2 {

  // A random number generator.
  var random = new Random(1234)

  // Number of different keys and values.
  var nkeys = 100
  var nvalues = 100

  // Fail rate for machines.
  var failRate = 0.001

  // Rate of read access.
  var readRate = 0.85

  // Number of machines.
  var nmachines = 42

  // Amount of machines per chunk
  var machineChunkSize = 6

  // Simulation time.
  var time = 0

  // Number of modifications.
  var nmodifications = 10000

  // Fake stores.
  var stores: Array[mutable.Map[String, String]] = (0 until nmachines).map(_ => mutable.Map[String, String]()).toArray

  // To track read-write performance.
  var readWrites: mutable.Map[(Int, Int), Int] = mutable.Map[(Int, Int), Int]()

  def simulateGet(key: String, machine: Int): Option[String] = {
    readWrites.update((machine, time), readWrites.getOrElse((machine, time), 0) + 1)
    stores(machine).get(key)
  }

  def simulateSet(key: String, value: String, machine: Int): Unit = {
    readWrites.update((machine, time), readWrites.getOrElse((machine, time), 0) + 1)
    stores(machine).put(key, value)
  }

  // Simulation: Run a simulation step.
  def simulate(): Unit = {
    time = time + 1
    for (machine <- 0 until nmachines) {
      if (random.nextDouble() < failRate) {
        // Full drop-out of this machine.
        stores(machine).clear()
      }
    }
  }

  def resetSimulation(seed: Long = 1234): Unit = {
    random = new Random(seed)
    time = 0
    stores = (0 until nmachines).map(_ => mutable.Map[String, String]()).toArray
    readWrites = mutable.Map[(Int, Int), Int]()
  }

  /*
    !!! For a better understanding, first read the documentation of set function !!!
    The get function checks for a given key, if there are any information found in the key value store
    Therefor it first calculates the possible position in the chunk and after that it iterates over all chunks to find a possible entry
    If there is no entry it return None
   */
  def get(key: String): Option[String] = {
    val posInChunk = Math.abs(key.hashCode) % machineChunkSize
    for(machine <- 0 until (nmachines / machineChunkSize)) {
      val chunkNumber = machine * machineChunkSize
      val option = simulateGet(key, posInChunk + chunkNumber)
      if(option.isDefined) return option
    }
    None
  }

  /*
    This function adds a new entry for a specific key in our key value store.
    The key value store is divided into many chunks. That allows us to minimize the workload on the individual machines.
    If we want to add a new entry, we randomly choose one chunk of machines and add our data at a specific position.
    This position is calculated by using the modulo operation and is always the same for the corresponding group of hashCodes.
    Although our data is only stored once, we will find a configuration, where this system works very well (compare pdf).
   */
  def set(key: String, value: String): Unit = {
    // TODO: Improve.
    val posInChunk = Math.abs(key.hashCode) % machineChunkSize // calculate position in chunk
    val chunkNumber = machineChunkSize * random.nextInt(nmachines/machineChunkSize) // randomly choose one chunk
    simulateSet(key, value, posInChunk + chunkNumber) // add data to determined machine
  }

  // Example experiments:
  def main(args: Array[String]): Unit = {
    println("-----------------------")
    failRate = 0.001
    resetSimulation()
    experiment()

    println("-----------------------")
    failRate = 0.001
    nmachines = 40
    machineChunkSize = 10
    resetSimulation()
    experiment()

    println("-----------------------")
    failRate = 0.001
    readRate = 0.5
    nmachines = 40
    machineChunkSize = 10
    resetSimulation()
    experiment()

    println("-----------------------")
    failRate = 0.001
    readRate = 0.40
    nmachines = 40
    machineChunkSize = 10
    resetSimulation()
    experiment()

    println("-----------------------")
    failRate = 0.05
    readRate = 0.40
    nmachines = 40
    machineChunkSize = 10
    resetSimulation()
    experiment()
  }

  def experiment(): Unit = {

    // State used to test implementation.
    val state = mutable.Map[String, String]()

    var inconsistent = 0
    var missing = 0

    for (modification <- 0 until nmodifications) { // Modification steps.

      // Decide between reading or writing.
      if (random.nextDouble() < readRate && state.nonEmpty) {

        // Pick a key already assigned.
        val key = random.shuffle(state.keys.toSeq).head
        val result = get(key)

        // Read the key and check if distributed storage is consistent with state.
        if (result.isEmpty) missing = missing + 1
        else if (state(key) != result.get) inconsistent = inconsistent + 1
      }
      else {

        // Pick a random new or old key.
        val key = "key_" + random.nextInt(nkeys)
        val value = "value_" + random.nextInt(nvalues)

        // Assign to random value.
        state.update(key, value)
        set(key, value)
      }

      simulate() // Simulate next time step and random drop-out.
    }

    // TODO: Feel free to dump stuff to a csv.
    println("Inconsistent reads: " + inconsistent + " (" + (inconsistent * 100 / nmodifications) + "%)")
    println("Missing reads: " + missing + " (" + (missing * 100 / nmodifications) + "%)")

    plotReadWrites()
  }

  // Helper to use 'reduceByKey'.
  implicit class BetterIterable[K, V](val xs: Iterable[(K, V)]) {
    def reduceByKey(op: (V, V) => V): Map[K, V] = xs.groupMapReduce(x => x._1)(x => x._2)(op)
  }

  def getReadWrites(step: Int = 2500): Map[Int, Int] = {
    val sumByMachineStep: Map[(Int, Int), Int] = readWrites.toSeq
      .map { case ((machine, time), count) => ((machine, time / step), count) }
      .reduceByKey((l, r) => l + r)

    val maxByStep = sumByMachineStep.toSeq
      .map { case ((machine, time), count) => (time, count) }
      .reduceByKey((l, r) => Math.max(l, r))

    maxByStep
  }

  // This can be used to present nice output.
  def plotReadWrites(step: Int = 2500): Unit = {
    for ((s, count) <- getReadWrites(step)) {
      println("For time [" + s * step + "," + (s + 1) * step + "] max rw is " + count)
    }
  }

}
