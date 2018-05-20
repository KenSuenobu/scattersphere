package com.scattersphere.core.util

import java.util.concurrent.CompletionException

import com.scattersphere.core.util.execution.JobExecutor
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class ExceptionJobTest extends FlatSpec with Matchers with LazyLogging {

  class TimerJob(duration: Int) extends Runnable {
    override def run(): Unit = {
      Thread.sleep(duration * 1000)
      logger.trace(s"Slept $duration second(s)")
      throw new NullPointerException
    }
  }

  "Exception Jobs" should "handle an exception" in {
    val runnableTask1: RunnableTask = RunnableTask(new TimerJob(3))
    val task1: Task = TaskBuilder()
        .withTask(RunnableTask(runnableTask1))
        .build()

    task1.status shouldBe TaskQueued
    task1.dependencies.length shouldBe 0

    val job1: Job = JobBuilder()
        .withName("Cancelable Job")
        .withTasks(task1)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 1
    job1.tasks(0) shouldBe task1
    job1.status shouldBe JobQueued

    val queuedJob: JobExecutor = jobExec.queue()

    queuedJob.setBlocking(false)
    jobExec.blocking shouldBe false
    queuedJob.run()
    queuedJob.setBlocking(true)
    job1.status shouldBe JobRunning

    println("Waiting 5 seconds before submitting a cancel.")
    Thread.sleep(5000)
    job1.status match {
      case JobFailed(_) => println("Job failed expected.")
      case x => fail(s"Unexpected job state caught: $x")
    }

    jobExec.blocking shouldBe true

    assertThrows[CompletionException] {
      queuedJob.run()
    }

    task1.status match {
      case TaskFailed(reason) => reason match {
        case _: NullPointerException => println(s"Expected NullPointerException caught.")
        case x => fail(s"Unexpected exception $x caught.")
      }

      case _ => fail("Expected NullPointerException.")
    }
  }

}
