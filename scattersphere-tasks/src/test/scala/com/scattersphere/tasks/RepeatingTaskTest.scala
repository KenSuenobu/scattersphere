package com.scattersphere.tasks

import java.util.concurrent.CompletionException

import com.scattersphere.core.util.execution.JobExecutor
import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{FlatSpec, Matchers}

class RepeatingTaskTest extends FlatSpec with Matchers with LazyLogging {

  "Repeating task" should "repeat a task 50 times" in {
    class TestRunnable extends RunnableTask {
      override def run(): Unit = {
        // Do nothing
      }
    }

    val rTask: RepeatingTask = new RepeatingTask(50, new TestRunnable)
    val task: Task = TaskBuilder().withName("Repeating task").withTask(rTask).build()
    val job: Job = JobBuilder().withTasks(task).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    rTask.getTimesRepeated() shouldBe 50
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
