/**
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

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
    val task1: Task = TaskBuilder("1").withTask(RunnableTask(new SleeperRunnable(1))).build()
    val task2: Task = TaskBuilder("2").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task1).build()
    val task3: Task = TaskBuilder("3").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task2).build()
    val task4: Task = TaskBuilder("4").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task3).build()
    val task5: Task = TaskBuilder("5").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task4).build()
    val job1: Job = JobBuilder("Timer Task").withTasks(task1, task2, task3, task4, task5).build()
    val jobExec: JobExecutor = JobExecutor(job1)

    assert(job1.id > 0)
    assert(task1.id > 0)
    assert(task2.id > task1.id)
    assert(task3.id > task2.id)
    assert(task4.id > task3.id)
    assert(task5.id > task4.id)
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
