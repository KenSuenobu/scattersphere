package com.scattersphere.core.util

import java.util.concurrent.CompletableFuture

import com.scattersphere.core.util.execution.JobExecutor
import org.scalatest.{FlatSpec, Matchers}

class CancelationJobTest extends FlatSpec with Matchers {

  class TimerJob(duration: Int) extends RunnableTask {
    override def run(): Unit = {
      Thread.sleep(duration * 1000)
      println(s"Slept $duration second(s)")
    }
  }

  "Cancelable Jobs" should "handle a cancel request" in {
    val runnableTask1: RunnableTask = new TimerJob(3)
    val runnableTask2: RunnableTask = new TimerJob(60)
    val task1: Task = new Task("3 second task", runnableTask1)
    val task2: Task = new Task("60 second task", runnableTask2)

    task1.getStatus shouldBe TaskQueued
    task2.getStatus shouldBe TaskQueued

    task1.name shouldBe "3 second task"
    task2.name shouldBe "60 second task"

    task1.getDependencies.length shouldBe 0
    task2.addDependency(task1)
    task2.getDependencies.length shouldBe 1

    val job1: Job = new Job("Cancelable Job", Seq(task1, task2))
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length shouldBe 2
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2

    val queuedJob: CompletableFuture[Void] = jobExec.queue()

    println("Waiting 5 seconds before submitting a cancel.")
    Thread.sleep(5000)

    queuedJob.cancel(true)
    queuedJob.join()

    task1.getStatus shouldBe TaskFinished
    task2.getStatus shouldBe TaskCanceled
  }

}
