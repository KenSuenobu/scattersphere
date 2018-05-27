package com.scattersphere.tasks

import com.scattersphere.core.util.execution.JobExecutor
import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class DelayedTaskTest extends FlatSpec with Matchers with LazyLogging {

  "Delayed Task" should "delay 5 seconds before running a job" in {
    class SleeperThread extends Runnable {
      override def run(): Unit = {
        println(s"Triggered!")
      }
    }

    var sTask = TaskBuilder()
      .withTask(new DelayedTask(2000, RunnableTask(new SleeperThread)))
      .withName("Delayed Task")
      .build()
    var sJob = JobBuilder()
      .withTasks(sTask)
      .build()
    var jExec: JobExecutor = JobExecutor(sJob)

    jExec.setBlocking(false).queue().run()

    logger.info("Sleeping 1 second before checking job and task status ...")
    Thread.sleep(1000)
    sJob.status shouldBe JobRunning
    sTask.status shouldBe TaskRunning

    logger.info("Allowing task to finish before checking job and task status ...")
    Thread.sleep(2000)
    sJob.status shouldBe JobFinished
    sTask.status shouldBe TaskFinished
  }

}
