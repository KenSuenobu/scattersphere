package com.scattersphere.tasks

import com.scattersphere.core.util.execution.JobExecutor
import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class ShellTaskTest extends FlatSpec with Matchers with LazyLogging {

  "Shell Task" should "be able to call /bin/ls -al /etc" in {
    val shellTask: ShellTask = new ShellTask("/bin/ls -al /etc/")
    val sTask1: Task = TaskBuilder().withName("shell task").withTask(shellTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    job.status shouldBe JobFinished

    val output: List[String] = shellTask.getProcessOutput().toList

    assert(output.size > 2)
    assert(output(0).toLowerCase() contains "total")
  }

}
