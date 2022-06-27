package org.softlang.bigdata.ass6

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

object MyDistributedKVStore {

  val AMOUNT_MACHINES = 4
  var storagePtr = 0

  // Fake distribution: These are 4 mutable stores (Maps) on 4 machines. Please pretend that they
  // are located on different machines and that accessing them invokes network traffic.
  // Consider this when using them.
  val stores: Array[mutable.Map[String, String]] = Array(
    mutable.Map[String, String](), // Store 0 on machine 0.
    mutable.Map[String, String](), // Store 1 on machine 1.
    mutable.Map[String, String](), // Store 2 on machine 2.
    mutable.Map[String, String]() // Store 3 on machine 3.
  )

  def get(key: String): String = {
    // TODO: Implement recovery of data from the distributed Maps.
    var ptr = 0
    var result: String = null
    var tmp: Option[String] = None
    while (result == null && ptr < AMOUNT_MACHINES) {
      tmp = stores(ptr).get(key)
      result = tmp.orNull
      ptr += 1
    }
    result
  }

  def set(key: String, value: String): Unit = {
    // One example of accessing a store:
    // Check the size of entries in store on machine 0: stores(0).size
    // TODO: Implement storage on the distributed Maps.
    stores(storagePtr).addOne((key, value))
    storagePtr = (storagePtr+1)%AMOUNT_MACHINES
  }

  // Example usage:
  def main(args: Array[String]): Unit = {
    // Store.
    set("key1", "value1")
    set("key2", "value2")
    set("key3", "value3")

    // Get.
    assert(get("key1") == "value1")
    assert(get("key2") == "value2")
    assert(get("key3") == "value3")

    // Rapid store test.
    for (i <- 1 to 1000) set("key" + i, "value" + i)
    for (i <- 1 to 1000) assert(get("key" + i) == "value" + i)
  }

}
