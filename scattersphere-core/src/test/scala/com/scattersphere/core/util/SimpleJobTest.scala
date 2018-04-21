package com.scattersphere.core.util

import org.scalatest.{FlatSpec, Matchers}

class SimpleJobTest extends FlatSpec with Matchers {

  class RunnableTask1 extends RunnableTask {
    private var initVars: Map[String, String] = Map()

    def init(vars: Map[String, String]): Unit = {
      initVars = vars
    }

    def run(): Unit = {
      val sleepTime = initVars.getOrElse("sleep", "10").toInt * 1000

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

  it should "be able to add a secondary task as a dependency" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)
    val task2: TaskDesc = new TaskDesc("Second Runnable Task", new RunnableTask1)

    task1.taskName shouldBe "First Runnable Task"
    task2.taskName shouldBe "Second Runnable Task"

    task1.addDependency(task2)
    task1.getDependencies.length equals 1
    task1.getDependencies.take(1) equals task2
  }

}
