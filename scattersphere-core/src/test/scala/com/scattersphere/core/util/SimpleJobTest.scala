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
class SimpleJobTest extends FlatSpec with Matchers {

  class RunnableTask1 extends RunnableTask {
    def run(): Unit = {
      val sleepTime = getSettings().getOrElse("sleep", "1").toInt * 1000

      println(s"Sleeping $sleepTime milliseconds.")
      Thread.sleep(sleepTime)
      println("Sleep thread completed.")
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
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length equals 1
    job1.tasks(0) equals task1

    jobExec.queue.get()
  }

}
