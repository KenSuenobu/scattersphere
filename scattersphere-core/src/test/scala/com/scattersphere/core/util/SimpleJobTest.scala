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

import com.scattersphere.core.util.execution.{InvalidTaskStateException, JobExecutor}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

/**
  * This is a really simple test - all it checks is that jobs can be created with tasks, the tasks run, and all of
  * the tasks run properly.  There is no exeception checking, there are no thrown exceptions, and no complicated
  * tasks that create a large DAG.  This code creates a simple set of DAGs: One that follows one after another, and
  * one that runs multiple tasks asynchronously.
  */
class SimpleJobTest extends FlatSpec with Matchers with LazyLogging {

  class RunnableTestTask(name: String) extends Runnable {
    var setVar: Int = 0

    override def run(): Unit = {
      val sleepTime = 100

      Thread.sleep(sleepTime)
      logger.trace(s"[$name] Sleep thread completed.")

      setVar = name.toInt
    }
  }

  /**
    * This task runs the following tests:
    *
    * Creates three tasks for a single job, naming each one uniquely.  Since each job runs one after another, the
    * job will queue up, and will automatically run after the first task starts asynchronously.  Since each task
    * has a 1 second wait period, we can check start/end variables.
    *
    * So, the job looks like this:
    *
    * Task 1 -> Task 2 -> Task 3
    *
    * Task 1 runs, then task 2, then task 3.
    *
    * The job itself is not complete until the last task finishes or an exception occurs.
    */
  "Simple Jobs" should "prepare a job and execute the first, second, and third tasks properly" in {
    val runnableTask1 = new RunnableTestTask("1") with RunnableTask
    val runnableTask2 = new RunnableTestTask("2") with RunnableTask
    val runnableTask3 = new RunnableTestTask("3") with RunnableTask
    val task1: Task = TaskBuilder()
        .withName("First Runnable Task")
        .withTask(runnableTask1)
        .build()
    val task2: Task = TaskBuilder()
        .withName("Second Runnable Task")
        .withTask(runnableTask2)
        .dependsOn(task1)
        .build()
    val task3: Task = TaskBuilder()
        .withName("Third Runnable Task")
        .withTask(runnableTask3)
        .dependsOn(task2)
        .build()

    task1.status shouldBe TaskQueued
    task2.status shouldBe TaskQueued
    task3.status shouldBe TaskQueued

    task1.name shouldBe "First Runnable Task"
    task1.dependencies.length shouldBe 0

    task2.name shouldBe "Second Runnable Task"
    task2.dependencies.length shouldBe 1

    task3.name shouldBe "Third Runnable Task"
    task3.dependencies.length shouldBe 1

    val job1: Job = JobBuilder()
        .withName("Test")
        .withTasks(task1, task2, task3)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 3
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    runnableTask1.setVar shouldBe 0
    runnableTask2.setVar shouldBe 0
    runnableTask3.setVar shouldBe 0

    job1.status shouldBe JobQueued
    jobExec.blocking shouldBe true
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe 1
    runnableTask2.setVar shouldBe 2
    runnableTask3.setVar shouldBe 3
    task1.status shouldBe TaskFinished
    task2.status shouldBe TaskFinished
    task3.status shouldBe TaskFinished
  }

  /**
    * This task is similar to the one above, but it runs all three tasks simultaneously.  Since there is no way to
    * guarantee execution order when this happens, we simply check value statuses at the end of the run.
    */
  it should "be able to run three tasks asynchronously" in {
    val runnableTask1 = new RunnableTestTask("1") with RunnableTask
    val runnableTask2 = new RunnableTestTask("2") with RunnableTask
    val runnableTask3 = new RunnableTestTask("3") with RunnableTask
    val task1: Task = TaskBuilder()
        .withName("First Runnable Task")
        .withTask(runnableTask1)
        .build()
    val task2: Task = TaskBuilder()
        .withName("Second Runnable Task")
        .withTask(runnableTask2)
        .build()
    val task3: Task = TaskBuilder()
        .withName("Third Runnable Task")
        .withTask(runnableTask3)
        .build()

    task1.status shouldBe TaskQueued
    task2.status shouldBe TaskQueued
    task3.status shouldBe TaskQueued

    task1.name shouldBe "First Runnable Task"
    task1.dependencies.length shouldBe 0

    task2.name shouldBe "Second Runnable Task"
    task2.dependencies.length shouldBe 0

    task3.name shouldBe "Third Runnable Task"
    task3.dependencies.length shouldBe 0

    val job1: Job = JobBuilder()
      .withName("Test")
      .withTasks(task1, task2, task3)
      .build()
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.status shouldBe JobQueued
    assert(job1.id > 0)
    assert(task1.id > 0)
    assert(task2.id > task1.id)
    assert(task3.id > task2.id)
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe 1
    runnableTask2.setVar shouldBe 2
    runnableTask3.setVar shouldBe 3
    task1.status shouldBe TaskFinished
    task2.status shouldBe TaskFinished
    task3.status shouldBe TaskFinished
  }

  it should "not allow the same task to exist on two separate jobs after completing in one job" in {
    val runnableTask1 = new RunnableTestTask("1") with RunnableTask
    val task1: Task = TaskBuilder()
        .withName("First Runnable Task")
        .withTask(runnableTask1)
        .build()

    task1.status shouldBe TaskQueued
    task1.name shouldBe "First Runnable Task"
    task1.dependencies.length shouldBe 0
    val job1: Job = JobBuilder()
        .withName("Test")
        .withTasks(task1)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.status shouldBe JobQueued
    assert(job1.id > 0)
    assert(task1.id > 0)
    jobExec.blocking shouldBe true
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe 1
    task1.status shouldBe TaskFinished

    val job2: Job = JobBuilder()
        .withName("Test2")
        .withTasks(task1)
        .build()
    val jobExec2: JobExecutor = JobExecutor(job2)

    // This will be upgraded soon so that the underlying cause can be pulled from the Job, but only if the job
    // completes exceptionally.
    jobExec2.blocking shouldBe true
    jobExec2.queue().run()
    job2.status match {
      case JobFailed(_) => println("Job failed, expected.")
      case x => fail(s"Unexpected job status: $x")
    }
    assert(job2.id > job1.id)

    task1.status match {
      case TaskFailed(reason) => reason match {
        case _: InvalidTaskStateException => println(s"Expected InvalidTaskStateException caught.")
        case x => fail(s"Unexpected exception $x caught.")
      }

      case _ => fail("Expected InvalidTaskStateException.")
    }
  }

  it should "be able to run a Task with a convenience method" in {
    val task1: Task = Task("Sleeper Timer") {
      Thread.sleep(500)
      logger.info("Sleep 500 ms")
      Thread.sleep(500)
      logger.info("Sleep another 500 ms")
    }
    val job1: Job = JobBuilder()
      .withTasks(task1)
      .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    jobExec.queue().run()
  }

  it should "be able to run asynchronous tasks with convenience" in {
    val task1: Task = Task("Sleeper Timer 1") {
      Thread.sleep(500)
      logger.info("Sleep 500 ms")
      Thread.sleep(500)
      logger.info("Sleep another 500 ms")
    }
    val task2: Task = Task.async("Sleeper Timer 2") {
      Thread.sleep(500)
      logger.info("Sleep 500 ms")
      Thread.sleep(1000)
      logger.info("Sleep 1000 ms")
    }
    val task3: Task = Task.async("Sleeper Timer 3") {
      Thread.sleep(500)
      logger.info("Sleep 500 ms")
      Thread.sleep(1000)
      logger.info("Sleep 1000 ms")
    }
    val job1: Job = JobBuilder()
      .withTasks(task1)
      .build()
    val jobExec: JobExecutor = JobExecutor(job1)
    assert(job1.id > 0)
    assert(task1.id > 0)
    assert(task2.id > task1.id)
    assert(task3.id > task2.id)

    jobExec.queue().run()

    val job2: Job = JobBuilder()
      .withTasks(task2, task3)
      .build()
    val jobExec2: JobExecutor = JobExecutor(job2)

    jobExec2.queue().run()
    assert(job2.id > 0)
  }

}
