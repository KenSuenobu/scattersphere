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
import org.scalatest.{FlatSpec, Matchers}

/**
  * SimpleJobTest
  *
  * This is a really simple test - all it checks is that jobs can be created with tasks, the tasks run, and all of
  * the tasks run properly.  There is no exeception checking, there are no thrown exceptions, and no complicated
  * tasks that create a large DAG.  This code creates a simple set of DAGs: One that follows one after another, and
  * one that runs multiple tasks asynchronously.
  */
class SimpleJobTest extends FlatSpec with Matchers  {

  class RunnableTask1 extends RunnableTask {
    def run(): Unit = {
      val sleepTime = getSettings().getOrElse("sleep", "1").toInt * 1000

      println(s"[1] Sleeping $sleepTime milliseconds.")
      Thread.sleep(sleepTime)
      println("[1] Sleep thread completed.")
    }
  }

  class RunnableTask2 extends RunnableTask {
    def run(): Unit = {
      val sleepTime = getSettings().getOrElse("sleep", "1").toInt * 1000

      println(s"[2] Sleeping $sleepTime milliseconds.")
      Thread.sleep(sleepTime)
      println("[2] Sleep thread completed.")
    }
  }

  "Simple Jobs" should "prepare a job and execute the first and second task properly" in {
    val task1: TaskDesc = new TaskDesc("First Runnable Task", new RunnableTask1)
    val task2: TaskDesc = new TaskDesc("Second Runnable Task", new RunnableTask2)

    task1.name shouldBe "First Runnable Task"
    task1.getDependencies.length equals 0
    task1.task.getStatus equals RunnableTaskStatus.QUEUED

    task2.name shouldBe "Second Runnable Task"
    task2.addDependency(task1)
    task2.getDependencies.length equals 1
    task2.task.getStatus equals RunnableTaskStatus.QUEUED

    val job1: JobDesc = new JobDesc("Test", Seq(task1, task2))
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length equals 2
    job1.tasks(0) equals task1
    job1.tasks(1) equals task2

    jobExec.queue.get()

    Thread.sleep(5000)
  }

}
