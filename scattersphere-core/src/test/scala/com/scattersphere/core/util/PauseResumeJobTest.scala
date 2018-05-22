package com.scattersphere.core.util

import com.scattersphere.core.util.execution.JobExecutor
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class PauseResumeJobTest extends FlatSpec with Matchers with LazyLogging {

  class SleeperRunnable(time: Int) extends Runnable {
    override def run(): Unit = {
      logger.info(s"Sleeping $time second(s).")
      Thread.sleep(time * 1000)
      logger.info("Sleep complete.")
    }
  }

  "Pausing and Resuming of Jobs" should "pause jobs properly" in {
    val task1: Task = TaskBuilder().withName("1").withTask(RunnableTask(new SleeperRunnable(1))).build()
    val task2: Task = TaskBuilder().withName("2").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task1).build()
    val task3: Task = TaskBuilder().withName("3").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task2).build()
    val task4: Task = TaskBuilder().withName("4").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task3).build()
    val task5: Task = TaskBuilder().withName("5").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task4).build()
    val job1: Job = JobBuilder().withName("Timer Task").withTasks(task1, task2, task3, task4, task5).build()
    val jobExec: JobExecutor = JobExecutor(job1)

    jobExec.setBlocking(false)
    jobExec.queue().run()
    Thread.sleep(2500)
    jobExec.pause()

    var numFinished: Int = 0
    var numQueued: Int = 0

    job1.tasks.foreach(_.status() match {
      case TaskFinished => numFinished += 1
      case TaskQueued => numQueued += 1
      case _ => // Do nothing
    })

    assert(numFinished > 0)
    assert(numQueued > 0)

    logger.info(s"Checkpoint: numFinished=$numFinished numQueued=$numQueued")

    Thread.sleep(2500)

    logger.info("Second checkpoint - still should not be finished, and queue should be in place.")
    numFinished = 0
    numQueued = 0

    job1.tasks.foreach(_.status() match {
      case TaskFinished => numFinished += 1
      case TaskQueued => numQueued += 1
      case _ => // Do nothing
    })

    assert(numFinished > 0)
    assert(numQueued > 0)

    logger.info("Waiting for the rest of the jobs to complete.")
    jobExec.setBlocking(true)
    jobExec.resume()
    jobExec.run()
    logger.info("Done.")
  }

}
