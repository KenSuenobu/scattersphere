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

package io.buildfactory.scattersphere.tasks

import java.util.concurrent.CompletionException

import com.typesafe.scalalogging.LazyLogging
import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import io.buildfactory.scattersphere.core.util._
import org.scalatest.{FlatSpec, Matchers}

class ShellTaskTest extends FlatSpec with Matchers with LazyLogging {

  "Shell Task" should "be able to call /bin/ls -al /etc" in {
    val shellTask: ShellTask = new ShellTask("/bin/ls -al /etc/")
    val sTask1: Task = TaskBuilder("shell task").withTask(shellTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    job.status shouldBe JobFinished

    val output: List[String] = shellTask.getProcessOutput().toList

    assert(output.size > 2)
    assert(output(0).toLowerCase() contains "total")
  }

  it should "handle commands that do not exist" in {
    val shellTask: ShellTask = new ShellTask("/unable/to/find/this command anywhere")
    val sTask1: Task = TaskBuilder("nonexistent task").withTask(shellTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    assertThrows[CompletionException] {
      jExec.queue().run()
    }

    job.status match {
      case JobFailed(x) => x match {
        case _: java.io.IOException => // Expected exception as well
        case x => fail("Unexpected exception occurred."); x.printStackTrace()
      }

      case x => fail(s"Unexpected job status: $x")
    }
  }

}
