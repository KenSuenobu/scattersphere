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

import java.util.concurrent.atomic.AtomicInteger

import com.scattersphere.core.util.execution.JobExecutor
import org.scalatest.{FlatSpec, Matchers}

/**
  * ComplicatedJobTest
  *
  * These tasks are more complicated.
  */
class ComplicatedJobTest extends FlatSpec with Matchers  {

  class RunnableTestTask(name: String) extends RunnableTask {
    var setVar: String = ""
    var callCount: AtomicInteger = new AtomicInteger(0)

    override def run(): Unit = {
      callCount.incrementAndGet

      val sleepTime = 500

      Thread.sleep(sleepTime)
      println(s"[$name] Sleep thread completed.")

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
    val task1: Task = new Task("First Task", runnableTask1)
    val task2: Task = new Task("Second Task", runnableTask2, true)
    val task3: Task = new Task("Third Task", runnableTask3, true)

    task1.getStatus shouldBe TaskQueued
    task2.getStatus shouldBe TaskQueued
    task3.getStatus shouldBe TaskQueued

    task1.name shouldBe "First Task"
    task1.getDependencies.length shouldBe 0

    task2.name shouldBe "Second Task"
    task2.addDependency(task1)
    task2.getDependencies.length shouldBe 1

    task3.name shouldBe "Third Task"
    task3.addDependency(task1)
    task3.getDependencies.length shouldBe 1

    val job1: Job = new Job("Test", Seq(task1, task2, task3))
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length shouldBe 3
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""

    job1.getStatus() shouldBe JobQueued
    jobExec.queue().run()
    job1.getStatus() shouldBe JobFinished

    runnableTask1.setVar shouldBe "1"
    runnableTask2.setVar shouldBe "2-A"
    runnableTask3.setVar shouldBe "2-B"
    runnableTask1.callCount.get() shouldBe 1
    runnableTask2.callCount.get() shouldBe 1
    runnableTask3.callCount.get() shouldBe 1

    task1.getStatus shouldBe TaskFinished
    task2.getStatus shouldBe TaskFinished
    task3.getStatus shouldBe TaskFinished
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
    val task1: Task = new Task("First Task", runnableTask1)
    val task2: Task = new Task("Second Task", runnableTask2, true)
    val task3: Task = new Task("Third Task", runnableTask3, true)
    val task4: Task = new Task("Fourth Task", runnableTask4)

    task1.getStatus shouldBe TaskQueued
    task2.getStatus shouldBe TaskQueued
    task3.getStatus shouldBe TaskQueued
    task4.getStatus shouldBe TaskQueued

    task1.name shouldBe "First Task"
    task1.getDependencies.length shouldBe 0

    // Task 2 requires task 1 to finish before starting.
    task2.name shouldBe "Second Task"
    task2.addDependency(task1)
    task2.getDependencies.length shouldBe 1

    // Task 3 requires task 1 to finish before starting.
    task3.name shouldBe "Third Task"
    task3.addDependency(task1)
    task3.getDependencies.length shouldBe 1

    // Task 4 requires task 2 and task 3 to finish before starting.
    task4.name shouldBe "Fourth Task"
    task4.addDependency(task2)
    task4.addDependency(task3)
    task4.getDependencies.length shouldBe 2

    val job1: Job = new Job("Test", Seq(task1, task2, task3, task4))
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length shouldBe 4
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    job1.tasks(3) shouldBe task4
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""
    runnableTask4.setVar shouldBe ""

    job1.getStatus() shouldBe JobQueued
    jobExec.queue().run()
    job1.getStatus() shouldBe JobFinished

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

    task1.getStatus shouldBe TaskFinished
    task2.getStatus shouldBe TaskFinished
    task3.getStatus shouldBe TaskFinished
    task4.getStatus shouldBe TaskFinished
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
    val task1: Task = new Task("1", runnableTask1)
    val task2: Task = new Task("2-A", runnableTask2)
    val task3: Task = new Task("2-B", runnableTask3)
    val task4: Task = new Task("3-A-2-B", runnableTask4, true)
    val task5: Task = new Task("3-B-2-B", runnableTask5, true)
    val task6: Task = new Task("4", runnableTask6)

    task1.getStatus shouldBe TaskQueued
    task2.getStatus shouldBe TaskQueued
    task3.getStatus shouldBe TaskQueued
    task4.getStatus shouldBe TaskQueued
    task5.getStatus shouldBe TaskQueued
    task6.getStatus shouldBe TaskQueued

    task1.name shouldBe "1"
    task1.getDependencies.length shouldBe 0

    // Task 2-A starts after 1 completes.
    task2.name shouldBe "2-A"
    task2.addDependency(task1)
    task2.getDependencies.length shouldBe 1

    // Task 2-B starts after 1 completes.
    task3.name shouldBe "2-B"
    task3.addDependency(task1)
    task3.getDependencies.length shouldBe 1

    // Task 3-A asynchronously starts after 2-B completes
    task4.name shouldBe "3-A-2-B"
    task4.addDependency(task3)
    task4.getDependencies.length shouldBe 1

    // Task 3-B asynchronously starts after 2-B completes
    task5.name shouldBe "3-B-2-B"
    task5.addDependency(task3)
    task5.getDependencies.length shouldBe 1

    // Task 4 starts after 2-A, 3-A and 3-B complete.
    task6.name shouldBe "4"
    task6.addDependency(task2)
    task6.addDependency(task4)
    task6.addDependency(task5)
    task6.getDependencies.length shouldBe 3

    val job1: Job = new Job("Test", Seq(task1, task2, task3, task4, task5, task6))
    val jobExec: JobExecutor = new JobExecutor(job1)

    job1.tasks.length shouldBe 6
    job1.tasks(0) shouldBe task1
    job1.tasks(1) shouldBe task2
    job1.tasks(2) shouldBe task3
    job1.tasks(3) shouldBe task4
    job1.tasks(4) shouldBe task5
    job1.tasks(5) shouldBe task6
    runnableTask1.setVar shouldBe ""
    runnableTask2.setVar shouldBe ""
    runnableTask3.setVar shouldBe ""
    runnableTask4.setVar shouldBe ""
    runnableTask5.setVar shouldBe ""
    runnableTask6.setVar shouldBe ""

    job1.getStatus() shouldBe JobQueued
    jobExec.queue().run()
    job1.getStatus() shouldBe JobFinished

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

    task1.getStatus shouldBe TaskFinished
    task2.getStatus shouldBe TaskFinished
    task3.getStatus shouldBe TaskFinished
    task4.getStatus shouldBe TaskFinished
    task5.getStatus shouldBe TaskFinished
    task6.getStatus shouldBe TaskFinished
  }

}
