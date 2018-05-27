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

class CancelJobTest extends FlatSpec with Matchers with LazyLogging {

  class SleeperRunnable(time: Int) extends Runnable {
    override def run(): Unit = {
      logger.info(s"Sleeping $time second(s).")
      Thread.sleep(time * 1000)
      logger.info("Sleep complete.")
    }
  }

  "Job Executor" should "be able to cancel a list of tasks" in {
    val task1: Task = TaskBuilder().withTask(RunnableTask(new SleeperRunnable(1))).withName("1").build()
    val task2: Task = TaskBuilder().withName("2").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task1).build()
    val task3: Task = TaskBuilder().withName("3").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task2).build()
    val task4: Task = TaskBuilder().withName("4").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task3).build()
    val task5: Task = TaskBuilder().withName("5").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task4).build()
    val task6: Task = TaskBuilder().withName("6").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task5).build()
    val task7: Task = TaskBuilder().withName("7").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task6).build()
    val task8: Task = TaskBuilder().withName("8").withTask(RunnableTask(new SleeperRunnable(1))).dependsOn(task7).build()
    val job1: Job = JobBuilder().withName("Timer Task").withTasks(task1, task2, task3, task4, task5, task6, task7, task8).build()
    val jobExec: JobExecutor = JobExecutor(job1)

    assert(job1.id > 0)
    jobExec.setBlocking(false)
    jobExec.queue().run()
    Thread.sleep(2500)
    jobExec.cancel("Canceling for testing sake.")

    Thread.sleep(2000)

    job1.status match {
      case JobCanceled(x) => x shouldBe "Canceling for testing sake."
      case x => fail(s"Unexpected job status matched: $x")
    }

    var taskFinished: Int = 0
    var taskCanceled: Int = 0

    job1.tasks.foreach(task => task.status() match {
      case TaskCanceled(_) => taskCanceled += 1
      case TaskFinished => taskFinished += 1
      case _ => // Ignored
    })

    assert(taskFinished > 0)
    assert(taskCanceled > 0)
  }

}
