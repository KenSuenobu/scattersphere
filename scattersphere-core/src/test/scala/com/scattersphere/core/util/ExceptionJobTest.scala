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

import java.util.concurrent.CompletionException

import com.scattersphere.core.util.execution.JobExecutor
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class ExceptionJobTest extends FlatSpec with Matchers with LazyLogging {

  class TimerJob(duration: Int) extends Runnable {
    override def run(): Unit = {
      Thread.sleep(duration * 1000)
      logger.trace(s"Slept $duration second(s)")
      throw new NullPointerException
    }
  }

  "Exception Jobs" should "handle an exception" in {
    val runnableTask1: RunnableTask = RunnableTask(new TimerJob(3))
    val task1: Task = TaskBuilder()
        .withName("Exception task")
        .withTask(RunnableTask(runnableTask1))
        .build()

    task1.status shouldBe TaskQueued
    task1.dependencies.length shouldBe 0

    val job1: Job = JobBuilder()
        .withName("Cancelable Job")
        .withTasks(task1)
        .build()
    val jobExec: JobExecutor = JobExecutor(job1)

    job1.tasks.length shouldBe 1
    job1.tasks(0) shouldBe task1
    job1.status shouldBe JobQueued

    val queuedJob: JobExecutor = jobExec.queue()

    queuedJob.setBlocking(false)
    jobExec.blocking shouldBe false
    queuedJob.run()
    queuedJob.setBlocking(true)
    job1.status shouldBe JobRunning

    println("Waiting 5 seconds before submitting a cancel.")
    Thread.sleep(5000)
    job1.status match {
      case JobFailed(_) => println("Job failed expected.")
      case x => fail(s"Unexpected job state caught: $x")
    }

    jobExec.blocking shouldBe true

    assertThrows[CompletionException] {
      queuedJob.run()
    }

    task1.status match {
      case TaskFailed(reason) => reason match {
        case _: NullPointerException => println(s"Expected NullPointerException caught.")
        case x => fail(s"Unexpected exception $x caught.")
      }

      case _ => fail("Expected NullPointerException.")
    }
  }

  it should "not let you build a task with a missing name" in {
    assertThrows[IllegalArgumentException] {
      TaskBuilder()
        .withTask(RunnableTask(new TimerJob(1)))
        .build()
    }
  }

  it should "not let you build a job without a task" in {
    assertThrows[IllegalArgumentException] {
      JobBuilder()
        .withName("My Job Name")
        .build()
    }
  }

}
