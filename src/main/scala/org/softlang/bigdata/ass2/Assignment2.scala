package org.softlang.bigdata.ass2

import java.io.FileWriter

import scala.util.Random
import scala.collection.mutable

object Assignment2 {

  // ------------------------ Start of the simulation code. ------------------------
  // Some flags.
  val FAILED = 0
  val READY = 1
  val PENDING = 2

  // A random number generator.
  var random = new Random(1234)

  // Variables that we use to track time and progress.
  var time = 0
  var completedTasks = 0

  // Number of machines that we have.
  var nMachines: Int = 10
  var todoTasks: Int = 20000
  var communication: Int = -100
  var failRate: Double = 0.01

  // Our abstracted cloud: number of tasks and done work on a certain machine.
  var cloud: mutable.Map[Int, (Int, Int)] = mutable.Map[Int, (Int, Int)]()

  for (i <- 0 until nMachines) cloud.put(i, (0, 0))

  // Simulation: Submit a number of tasks to a machine.
  def submit(machine: Int, nTasks: Int): Unit =
    cloud.put(machine, (nTasks, communication))

  // Simulation: Get the status of a machine (can be FAILED, READY or PENDING)
  def status(machine: Int): Int = {
    val (task, work) = cloud(machine)
    if (work == Int.MinValue) return FAILED
    if (work >= task) return READY
    PENDING
  }

  // Simulation: Run a single simulation step.
  def simulate(): Unit = {
    // Simulate one computation step on each machine.
    time = time + 1

    for (machine <- 0 until nMachines if status(machine) == PENDING) {
      // Simulate computation and transmission.
      val (task, work) = cloud(machine)

      cloud.put(machine, (task, work + 1))

      // Randomly fail at failRate of the computation steps.
      if (random.nextDouble() < failRate && work > 0)
        cloud.put(machine, (task, Int.MinValue))
      // If not failed, ddd the competed tasks if full batch is ready.
      else if (task == work + 1) completedTasks = completedTasks + task
    }
  }

  // Simulation: Reset simulation.
  def reset(seed: Int = 1234): Unit = {
    time = 0
    completedTasks = 0
    random = new Random(seed)
    cloud = mutable.Map[Int, (Int, Int)]()
    for (i <- 0 until nMachines) cloud.put(i, (0, 0))
  }

  // Simulation: Print current progress.
  def printProgress(): Unit =
    println("Completed " + completedTasks + " tasks out of " + todoTasks + " on time step " + time + ".")








  // cloud computing realisation
  def ccRealisation(nTasks: Int): mutable.Map[Int, Int] = {
    // measures the total execution time for all tasks depending on the given amount of tasks per machine
    val measurer: mutable.Map[Int, Int] = mutable.Map()

    // runs the cc algorithm for 1 to n tasks per machine
    for(n <- 1 to nTasks) {
      println("Run with " + n + " tasks per machine")
      // reset all cloud params
      reset()
      // calls execution method for each amount of tasks per machine and adds the execution time to measurer map.
      measurer.addOne(n, execution(n))
    }
    // returns map with measured execution times for each submitted amount of tasks
    measurer
  }

  // cloud computing algorithm that submits nTasks to each machine
  def execution(nTasks: Int): Int = {
    reset()
    // contains all not PENDING machines. At the beginning, all machines have the state READY.
    val unusedMachines: mutable.Set[Int] = mutable.Set.tabulate(nMachines)(n => n)
    // contains all machines running a task currently (PENDING). At the beginning, no machine is running a task.
    val usedMachines: mutable.Set[Int] = mutable.Set()

    // loop that ends when all tasks are done
    while (completedTasks < todoTasks) {
      // all already used machines, that are available for a new task execution will be added to corresponding set.
      usedMachines.foreach(machine => {
        if (status(machine) == READY || status(machine) == FAILED) {
          unusedMachines.add(machine)
        }
      })

      // all machines that aren't pending will be used to execute new tasks and marked as used (by moving them form unusedMachines set to usedMachines set)
      unusedMachines.foreach(machine => {
        submit(machine, nTasks)
        unusedMachines.remove(machine)
        usedMachines.add(machine)
      })

      // run simulation
      simulate()
    }
    // returns the needed execution time
    time
  }

  // writes given map into csv file
  def serialize(fileName: String, values: mutable.Map[Int, Int]): Unit = {
    val fw = new FileWriter("res/ass2/" + fileName + ".csv")
    fw.write("tasks, time \n")
    for((tasks, time) <- values) {
      fw.write(tasks + "," + time + "\n")
    }
    fw.flush()
    fw.close()
  }

  // ------------------------ End of the simulation code. ------------------------
  def main(args: Array[String]): Unit = {
    // Our naive approach to completing the tasks in the simulated cloud.
    // TODO: Please improve the following (delete if necessary).

   serialize(
      "default",
      ccRealisation(100)
    )


    nMachines = 20
    serialize(
      "machines",
      ccRealisation(100)
    )


    nMachines = 10
    communication = -25
    serialize(
      "communication",
      ccRealisation(100)
    )


    communication = -100
    failRate = 0.1
    serialize(
      "failrate",
      ccRealisation(50)
    )


    nMachines = 20
    communication = -25
    serialize(
      "correlate",
      ccRealisation(50)
    )
  }
}
