package com.scattersphere.core.util

import com.scattersphere.core.util.execution.{ExecutionEngine, JobExecutor}
import org.scalatest.{FlatSpec, Matchers}

class SimpleJobTest extends FlatSpec with Matchers {

  class RunnableTask1 extends RunnableTask {
    def run(): Unit = {
      val sleepTime = getSettings().getOrElse("sleep", "3").toInt * 1000

      println(s"Sleeping $sleepTime milliseconds.")
      Thread.sleep(sleepTime)
      println("Sleep complete; task completed.")
    }
  }

  "Simple Tasks" should "prepare properly" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)

    task1.name shouldBe "First Runnable Task"
    task1.getDependencies.length equals 0
    task1.task.getStatus equals RunnableTaskStatus.QUEUED
  }

  it should "not be able to add a task to itself as a dependency" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)

    assertThrows[IllegalArgumentException] {
      task1.addDependency(task1)
    }
  }

  it should "prepare a job and execute the first task properly" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)

    task1.name shouldBe "First Runnable Task"
    task1.getDependencies.length equals 0
    task1.task.getStatus equals RunnableTaskStatus.QUEUED

    val job1: JobDesc = new JobDesc("Test", Seq(task1))
    val jobExec: JobExecutor = new JobExecutor(ExecutionEngine("scala"), job1)

    job1.tasks.length equals 1
    job1.tasks(0) equals task1

    jobExec.queue.get()
  }

}
