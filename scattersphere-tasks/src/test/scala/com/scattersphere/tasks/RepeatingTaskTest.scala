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

package com.scattersphere.tasks

import java.util.concurrent.CompletionException

import com.scattersphere.core.util.execution.JobExecutor
import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class RepeatingTaskTest extends FlatSpec with Matchers with LazyLogging {

  "Repeating task" should "repeat a task 20 times" in {
    class TestRunnable extends RunnableTask {
      override def run(): Unit = {
        // Do nothing
      }
    }

    val rTask: RepeatingTask = new RepeatingTask(20, new TestRunnable)
    val task: Task = TaskBuilder().withName("Repeating task").withTask(rTask).build()
    val job: Job = JobBuilder().withTasks(task).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    rTask.getTimesRepeated() shouldBe 20
    job.status shouldBe JobFinished
  }

  it should "handle exceptions after 25 calls" in {
    var counter: Int = 0

    class TestRunnable extends RunnableTask {
      override def run(): Unit = {
        counter += 1

        if (counter > 25) {
          throw new NullPointerException
        }
      }
    }

    val rTask: RepeatingTask = new RepeatingTask(50, new TestRunnable)
    val task: Task = TaskBuilder().withName("Repeating task").withTask(rTask).build()
    val job: Job = JobBuilder().withTasks(task).build()
    val jExec: JobExecutor = JobExecutor(job)

    assertThrows[CompletionException] {
      jExec.queue().run()
    }

    rTask.getTimesRepeated() shouldBe 25

    job.status match {
      case JobFailed(x) => x match {
        case _: java.lang.NullPointerException => // Expected exception.
        case x => fail(s"Unexpected exception: ${x}")
      }

      case x => fail(s"Unexpected status: ${x}")
    }
  }

}
