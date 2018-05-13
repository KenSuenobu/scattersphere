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
package com.scattersphere.core.util.execution

import java.util.concurrent.{CompletionException, _}
import java.util.function.{Function => JavaFunction}

import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

/**
  * This is the heart of the execution engine.  It takes a [[Job]] object, traverses all of the [[Task]]
  * items defined in it, and creates a DAG.  From this DAG, it determines which tasks can be run asynchronously,
  * and which tasks have dependencies.
  *
  * Any tasks that contain dependencies will block until the task they depend on has completed.  Tasks that are
  * "root" tasks (ie. top level tasks) can be run asynchronously as multiple tasks in multiple threads as they
  * see fit.
  *
  * @param job The [[Job]] containing all of the tasks (and dependencies) to run.
  */
class JobExecutor(job: Job) extends LazyLogging {

  private lazy val executorService: PausableThreadPoolExecutor = PausableThreadPoolExecutor()

  private val taskMap: mutable.HashMap[String, CompletableFuture[Void]] = new mutable.HashMap
  private val lockObject: Object = new Object
  private var blocking: Boolean = true

  private var completableFuture: CompletableFuture[Void] = _

  executorService.pause()

  /**
    * Walks the tree of all tasks for this job, creating an execution DAG.  Since the top-level tasks run using an
    * asynchronous CompletableFuture, it's possible that the tasks will start while the DAG is being generated.
    * This should not affect how the tasks run, however, it may affect synchronization in your top-level application,
    * should you depend on timing or anything of that sort.
    *
    * @return CompletableFuture containing the completed DAG of tasks to execute.
    */
  def queue(): JobExecutor = {
    val tasks: Seq[Task] = job.tasks

    generateExecutionPlan(tasks)

    completableFuture = CompletableFuture
      .allOf(taskMap.values.toSeq: _*)
      .whenComplete((_, _) => {
        job.status() match {
          case JobRunning => job.setStatus(JobFinished)
          case _ => // Do nothing; keep state stored
        }

        executorService.shutdown
        logger.trace("Execution service shut down.")
      })

    this
  }

  def setBlocking(flag: Boolean) = {
    if (!flag) {
      unlock()
    }

    blocking = flag
  }

  def isBlocking(): Boolean = blocking

  def run(): Unit = {
    job.setStatus(JobRunning)
    executorService.resume()

    if (blocking) {
      completableFuture.join
    }
  }

  private def unlock(): Unit = {
    executorService.resume()
  }

  private def runTask(task: Task): Unit = {
    logger.info(s"Running task: $task")

    task.status match {
      case TaskQueued => {
        task.setStatus(TaskRunning)
        task.task.run()
        task.task.onFinished()
        task.setStatus(TaskFinished)
      }

      case _ => {
        val failedTaskException = new InvalidTaskStateException(task, task.status, TaskQueued)

        task.setStatus(TaskFailed(failedTaskException))
        job.setStatus(JobFailed(failedTaskException))
      }
    }
  }

  private def runExceptionally(task: Task, f: Throwable): Void = {
    f match {
      case ex: CompletionException => {
        task.task.onException(ex.getCause)
        task.setStatus(TaskFailed(ex.getCause))
        job.setStatus(JobFailed(ex.getCause))
      }
      case _ => {
        task.task.onException(f)
        task.setStatus(TaskFailed(f))
        job.setStatus(JobFailed(f))
      }
    }

    throw f
  }

  private def toJavaFunction[A, B](f: Function1[A, B]) = new JavaFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  private def walkSubtasks(dependent: Task, tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      taskMap.get(dependent.name) match {
        case Some(_) => logger.debug(s"[${dependent.name}: Already queued] Parent=${task.name} has ${task.dependencies.length} subtasks.")
        case None => {
          val parentFuture: CompletableFuture[Void] = taskMap(task.name)

          if (dependent.async) {
            val cFuture: CompletableFuture[Void] = parentFuture.thenRunAsync(() => runTask(dependent), executorService)

            taskMap.put(dependent.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
              runExceptionally(dependent, f))))

            logger.debug(s"[${dependent.name}: Queued (ASYNC)] Parent=${task.name} has ${task.dependencies.length} subtasks.")
          } else {
            val cFuture: CompletableFuture[Void] = parentFuture.thenRun(() => runTask(dependent))

            taskMap.put(dependent.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
              runExceptionally(dependent, f))))

            logger.debug(s"[${dependent.name}: Queued] Parent=${task.name} has ${task.dependencies.length} subtasks.")
          }
        }
      }

      if (task.dependencies.nonEmpty) {
        walkSubtasks(task, task.dependencies)
      }
    })
  }

  private def generateExecutionPlan(tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      if (task.dependencies.isEmpty) {
        logger.debug(s"Task: ${task.name} [ASYNC Root Task]")

        val cFuture: CompletableFuture[Void] = CompletableFuture.runAsync(() => runTask(task), executorService)

        taskMap.put(task.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
          runExceptionally(task, f))))
      } else {
        logger.debug(s"Task: ${task.name} task - Walking tree")
        walkSubtasks(task, task.dependencies)
      }
    })

    logger.info(s"Known task map: ${taskMap.keys}")
  }

}

class InvalidTaskStateException(task: Task,
                                status: TaskStatus,
                                expected: TaskStatus)
  extends Exception(s"InvalidTaskStateException: task ${task.name} set to $status, expected $expected")