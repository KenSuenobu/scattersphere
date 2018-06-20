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
package io.buildfactory.scattersphere.core.util

import java.util.concurrent.atomic.AtomicInteger

import com.typesafe.scalalogging.LazyLogging
import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import org.scalatest.{FlatSpec, Matchers}

/**
  * ComplicatedJobTest
  *
  * These tasks are more complicated.
  */
class ComplicatedJobTest extends FlatSpec with Matchers with LazyLogging {

  class RunnableTestTask(name: String) extends Runnable {
    var setVar: String = ""
    var callCount: AtomicInteger = new AtomicInteger(0)

    override def run(): Unit = {
      callCount.incrementAndGet

      val sleepTime = 100

      Thread.sleep(sleepTime)
      logger.trace(s"[$name] Sleep thread completed.")

      setVar = name
    }
  }

  /**
    * This complicated job isn't exactly too difficult to decypher.  It creates a DAG like this:
    *
    *      /-> Task 2-A
    * Task 1
    *      \-> Task 2-B
    *
    * Where Task 1 runs, then starts Tasks 2-A and Tasks 2-B at the same time.
    */
  "Complicated Jobs" should "prepare a job and execute the DAG" in {
    val runnableTask1 = new RunnableTestTask("1")
    val runnableTask2 = new RunnableTestTask("2-A")
    val runnableTask3 = new RunnableTestTask("2-B")
    val task1: Task = TaskBuilder("First Task")
        .withTask(RunnableTask(runnableTask1))
        .build()
    val task2: Task = TaskBuilder("Second Task")
        .withTask(RunnableTask(runnableTask2))
        .dependsOn(task1)
        .async()
        .build()
    val task3: Task = TaskBuilder("Third Task")
        .withTask(RunnableTask(runnableTask3))
        .dependsOn(task1)
        .async()
        .build()

    task1.status shouldBe TaskQueued
    task2.status shouldBe TaskQueued
    task3.status shouldBe TaskQueued

    task1.name shouldBe "First Task"
    task1.dependencies.length shouldBe 0

    task2.name shouldBe "Second Task"
    task2.dependencies.length shouldBe 1

    task3.name shouldBe "Third Task"
    task3.dependencies.length shouldBe 1

    val job1: Job = JobBuilder("Test")
        .withTasks(task1, task2, task3)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 3
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    assert(job1.id > 0)
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""

    job1.status shouldBe JobQueued
    jobExec.blocking shouldBe true
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe "1"
    runnableTask2.setVar shouldBe "2-A"
    runnableTask3.setVar shouldBe "2-B"
    runnableTask1.callCount.get() shouldBe 1
    runnableTask2.callCount.get() shouldBe 1
    runnableTask3.callCount.get() shouldBe 1

    task1.status shouldBe TaskFinished
    task2.status shouldBe TaskFinished
    task3.status shouldBe TaskFinished
  }

  /**
    * This complicated job isn't exactly too difficult to decypher.  It creates a DAG like this:
    *
    *      /-> Task 2-A -\
    * Task 1             -> Task 3
    *      \-> Task 2-B -/
    *
    * Where Task 1 runs, then starts Tasks 2-A and Tasks 2-B at the same time.  Task 3 waits for Tasks 2-A and 2-B
    * to complete before running.
    *
    * This job test is far more realistic: Task 1 could be an initialization task.  Tasks 2-A and Tasks 2-B could be
    * tasks that execute asynchronous fetches, and Task 3 brings all of the task data back together and loads that into
    * a database.
    */
  it should "prepare a more realistic job and execute the DAG" in {
    val runnableTask1 = new RunnableTestTask("1")
    val runnableTask2 = new RunnableTestTask("2-A")
    val runnableTask3 = new RunnableTestTask("2-B")
    val runnableTask4 = new RunnableTestTask("3")
    val task1: Task = TaskBuilder("First Task")
        .withTask(RunnableTask(runnableTask1))
        .build()
    val task2: Task = TaskBuilder("Second Task")
        .withTask(RunnableTask(runnableTask2))
        .dependsOn(task1)
        .async()
        .build()
    val task3: Task = TaskBuilder("Third Task")
        .withTask(RunnableTask(runnableTask3))
        .dependsOn(task1)
        .async()
        .build()
    val task4: Task = TaskBuilder("Fourth Task")
        .withTask(RunnableTask(runnableTask4))
        .dependsOn(task2)
        .dependsOn(task3)
        .build()

    task1.status shouldBe TaskQueued
    task2.status shouldBe TaskQueued
    task3.status shouldBe TaskQueued
    task4.status shouldBe TaskQueued

    task1.name shouldBe "First Task"
    task1.dependencies.length shouldBe 0

    // Task 2 requires task 1 to finish before starting.
    task2.name shouldBe "Second Task"
    task2.dependencies.length shouldBe 1

    // Task 3 requires task 1 to finish before starting.
    task3.name shouldBe "Third Task"
    task3.dependencies.length shouldBe 1

    // Task 4 requires task 2 and task 3 to finish before starting.
    task4.name shouldBe "Fourth Task"
    task4.dependencies.length shouldBe 2

    val job1: Job = JobBuilder("Test")
        .withTasks(task1, task2, task3, task4)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 4
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    job1.tasks(3) shouldBe task4
    assert(job1.id > 0)
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""
    runnableTask4.setVar shouldBe ""

    job1.status shouldBe JobQueued
    jobExec.blocking shouldBe true
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe "1"
    runnableTask2.setVar shouldBe "2-A"
    runnableTask3.setVar shouldBe "2-B"
    runnableTask4.setVar shouldBe "3"

    // Since we have a job that splits from 1 task to 2, then to 1, we want to make sure the last task doesn't
    // get erroneously called twice!
    runnableTask1.callCount.get() shouldBe 1
    runnableTask2.callCount.get() shouldBe 1
    runnableTask3.callCount.get() shouldBe 1
    runnableTask4.callCount.get() shouldBe 1

    task1.status shouldBe TaskFinished
    task2.status shouldBe TaskFinished
    task3.status shouldBe TaskFinished
    task4.status shouldBe TaskFinished
  }

  /**
    * It creates a DAG like this, and is able to test:
    *
    *      /-> Task 2-A ---------------------\
    * Task 1              -> Task 3-A-2-B -\  \
    *      \-> Task 2-B -|                  /--> Task 4
    *                     -> Task 3-B-2-B -/
    */
  it should "work with multiple dependencies" in {
    val runnableTask1 = new RunnableTestTask("1")
    val runnableTask2 = new RunnableTestTask("2-A")
    val runnableTask3 = new RunnableTestTask("2-B")
    val runnableTask4 = new RunnableTestTask("3-A-2-B")
    val runnableTask5 = new RunnableTestTask("3-B-2-B")
    val runnableTask6 = new RunnableTestTask("4")
    val task1: Task = TaskBuilder("1")
        .withTask(RunnableTask(runnableTask1))
        .build()
    val task2: Task = TaskBuilder("2-A")
        .withTask(RunnableTask(runnableTask2))
        .dependsOn(task1)
        .build()
    val task3: Task = TaskBuilder("2-B")
        .withTask(RunnableTask(runnableTask3))
        .dependsOn(task1)
        .build()
    val task4: Task = TaskBuilder("3-A-2-B")
        .withTask(RunnableTask(runnableTask4))
        .dependsOn(task3)
        .async()
        .build()
    val task5: Task = TaskBuilder("3-B-2-B")
        .withTask(RunnableTask(runnableTask5))
        .dependsOn(task3)
        .async()
        .build()
    val task6: Task = TaskBuilder("4")
        .withTask(RunnableTask(runnableTask6))
        .dependsOn(task2, task4, task5)
        .build()

    task1.status shouldBe TaskQueued
    task2.status shouldBe TaskQueued
    task3.status shouldBe TaskQueued
    task4.status shouldBe TaskQueued
    task5.status shouldBe TaskQueued
    task6.status shouldBe TaskQueued

    task1.name shouldBe "1"
    task1.dependencies.length shouldBe 0

    // Task 2-A starts after 1 completes.
    task2.name shouldBe "2-A"
    task2.dependencies.length shouldBe 1

    // Task 2-B starts after 1 completes.
    task3.name shouldBe "2-B"
    task3.dependencies.length shouldBe 1

    // Task 3-A asynchronously starts after 2-B completes
    task4.name shouldBe "3-A-2-B"
    task4.dependencies.length shouldBe 1

    // Task 3-B asynchronously starts after 2-B completes
    task5.name shouldBe "3-B-2-B"
    task5.dependencies.length shouldBe 1

    // Task 4 starts after 2-A, 3-A and 3-B complete.
    task6.name shouldBe "4"
    task6.dependencies.length shouldBe 3

    val job1: Job = JobBuilder("Test")
        .withTasks(task1, task2, task3, task4, task5, task6)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 6
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    job1.tasks(3) shouldBe task4
    job1.tasks(4) shouldBe task5
    job1.tasks(5) shouldBe task6
    assert(task1.id > 0)
    assert(task2.id > 0)
    assert(task3.id > 0)
    assert(task4.id > 0)
    assert(task5.id > 0)
    assert(task6.id > 0)
    assert(job1.id > 0)
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""
    runnableTask4.setVar shouldBe ""
    runnableTask5.setVar shouldBe ""
    runnableTask6.setVar shouldBe ""

    job1.status shouldBe JobQueued
    jobExec.blocking shouldBe true
    jobExec.queue().run()
    job1.status shouldBe JobFinished

    runnableTask1.setVar shouldBe "1"
    runnableTask2.setVar shouldBe "2-A"
    runnableTask3.setVar shouldBe "2-B"
    runnableTask4.setVar shouldBe "3-A-2-B"
    runnableTask5.setVar shouldBe "3-B-2-B"
    runnableTask6.setVar shouldBe "4"

    // Since we have a job that splits from 1 task to 2, then to 1, we want to make sure the last task doesn't
    // get erroneously called twice!
    runnableTask1.callCount.get() shouldBe 1
    runnableTask2.callCount.get() shouldBe 1
    runnableTask3.callCount.get() shouldBe 1
    runnableTask4.callCount.get() shouldBe 1
    runnableTask5.callCount.get() shouldBe 1
    runnableTask6.callCount.get() shouldBe 1

    task1.status shouldBe TaskFinished
    task2.status shouldBe TaskFinished
    task3.status shouldBe TaskFinished
    task4.status shouldBe TaskFinished
    task5.status shouldBe TaskFinished
    task6.status shouldBe TaskFinished
  }

}
