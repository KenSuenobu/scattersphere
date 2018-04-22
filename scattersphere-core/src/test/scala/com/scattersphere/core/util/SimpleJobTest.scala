package com.scattersphere.core.util

import org.scalatest.{FlatSpec, Matchers}

class SimpleJobTest extends FlatSpec with Matchers {

  class RunnableTask1 extends RunnableTask {
    def run(): Unit = {
      val sleepTime = getSettings().getOrElse("sleep", "10").toInt * 1000

      println(s"Sleeping $sleepTime milliseconds.")
      Thread.sleep(sleepTime)
      println("Sleep complete; task completed.")
    }
  }

  "Simple Tasks" should "prepare properly" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)

    task1.taskName shouldBe "First Runnable Task"
    task1.getDependencies.length equals 0
  }

  it should "not be able to add a job to itself as a dependency" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)

    assertThrows[IllegalArgumentException] {
      task1.addDependency(task1)
    }
  }

  // Task1 <- Task2
  it should "be able to add a secondary task as a dependency" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)
    val task2: TaskDesc = new TaskDesc("Second Runnable Task", new RunnableTask1)

    task1.taskName shouldBe "First Runnable Task"
    task2.taskName shouldBe "Second Runnable Task"

    // Adding task1 as a dependency on task2 means that task1 must complete before task2
    // can start.
    task2.addDependency(task1)
    task2.getDependencies.length equals 1
    task2.getDependencies.take(1) equals task1
  }

  // Task1 <- Task2 <- Task3
  it should "be able to add a secondary and tertiary task as a dependency" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)
    val task2: TaskDesc = new TaskDesc("Second Runnable Task", new RunnableTask1)
    val task3: TaskDesc = new TaskDesc("Third Runnable Task", new RunnableTask1)

    task1.taskName shouldBe "First Runnable Task"
    task2.taskName shouldBe "Second Runnable Task"
    task3.taskName shouldBe "Third Runnable Task"

    task3.addDependency(task2)
    task3.getDependencies.length equals 1
    task3.getDependencies.take(1) equals task2

    task2.addDependency(task1)
    task2.getDependencies.length equals 1
    task2.getDependencies.take(1) equals task1
  }

}
