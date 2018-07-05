/*
 *    _____            __  __                       __
 *   / ___/_________ _/ /_/ /____  ______________  / /_  ___  ________
 *   \__ \/ ___/ __ `/ __/ __/ _ \/ ___/ ___/ __ \/ __ \/ _ \/ ___/ _ \
 *  ___/ / /__/ /_/ / /_/ /_/  __/ /  (__  ) /_/ / / / /  __/ /  /  __/
 * /____/\___/\__,_/\__/\__/\___/_/  /____/ .___/_/ /_/\___/_/   \___/
 *                                       /_/
 *
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
package io.buildfactory.scattersphere.core.util.execution

import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletionException, _}
import java.util.function.{Function => JavaFunction}

import io.buildfactory.scattersphere.core.util._
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger

import scala.collection.mutable

/** This is the heart of the execution engine.  It takes a [[Job]] object, traverses all of the [[Task]]
  * items defined in it, and creates a DAG.  From this DAG, it determines which tasks can be run asynchronously,
  * and which tasks have dependencies.
  *
  * Any tasks that contain dependencies will block until the task they depend on has completed.  Tasks that are
  * "root" tasks (ie. top level tasks) can be run asynchronously as multiple tasks in multiple threads as they
  * see fit.
  *
  * Contains some code adapted from Matthew A. Johnston (warmwaffles) for PausableThreadPoolExecutor.  Although that
  * code is no longer used here, the logic for said code does exist here.
  *
  * @param job The [[Job]] containing all of the tasks (and dependencies) to run.
  * @since 0.0.1
  */
class JobExecutor(job: Job) extends SimpleLogger {

  private lazy val executorService: ThreadPoolExecutor = new ThreadPoolExecutor(Runtime.getRuntime.availableProcessors(),
                                                                                Runtime.getRuntime.availableProcessors() * 10,
                                                                                Long.MaxValue,
                                                                                TimeUnit.SECONDS,
                                                                                new LinkedBlockingQueue[Runnable]())
  private type TaskMap = mutable.HashMap[String, CompletableFuture[Void]]

  private val taskMap: TaskMap = new TaskMap
  private var isBlocking: Boolean = true

  private val lock: ReentrantLock = new ReentrantLock()
  private val condition: Condition = lock.newCondition()
  private var isPaused = true

  private var completableFuture: CompletableFuture[Void] = null

  class WhenComplete extends java.util.function.BiConsumer[Void, Throwable] {
    override def accept(t: Void, u: Throwable): Unit = {
      if (job.status.equals(JobRunning)) {
        job.setStatus(JobFinished)
      }

      job.getStatistics().triggerEnd()

      logger.info(s"Job ${job.name} finished: elapsed time ${job.getStatistics().getElapsedTime}ms")

      executorService.shutdown
      logger.trace("Execution service shut down.")
    }
  }

  /** Walks the tree of all tasks for this job, creating an execution DAG.  Since the top-level tasks run using an
    * asynchronous CompletableFuture, it's possible that the tasks will start while the DAG is being generated.
    * This should not affect how the tasks run, however, it may affect synchronization in your top-level application,
    * should you depend on timing or anything of that sort.
    *
    * @return this object
    * @throws InvalidJobExecutionStateException if queue called more than once
    */
  def queue(): JobExecutor = {
    if (completableFuture != null) {
      throw new InvalidJobExecutionStateException("Cannot re-queue the same job.")
    }

    val tasks: Seq[Task] = job.tasks

    generateExecutionPlan(tasks)

    completableFuture = CompletableFuture
      .allOf(taskMap.values.toSeq: _*)
      .whenComplete(new WhenComplete())

    this
  }

  /** When set true, the run method will block until the entire DAG executes completely.
    *
    * @param flag boolean indicating whether or not to block on execution.
    * @return this object
    */
  def setBlocking(flag: Boolean): JobExecutor = {
    if (!flag) {
      resume()
    }

    isBlocking = flag
    this
  }

  /** Retrieves the underlying CompletableFuture.
    *
    * @return the CompletableFuture
    * @since 0.0.2
    */
  def getCompletableFuture(): CompletableFuture[Void] = completableFuture

  /** Indicates whether or not the `run()` method should block until completion. */
  def blocking: Boolean = isBlocking

  /** Executes the DAG. When set to non-blocking, this will start the DAG and will return immediately.
    *
    * @throws InvalidJobExecutionStateException if not queued first
    */
  def run(): Unit = {
    if (completableFuture == null) {
      throw new InvalidJobExecutionStateException("Called out of order - queue required.")
    }

    job.getStatistics().triggerStart()
    job.setStatus(JobRunning)

    if (isPaused) {
      resume()
    }

    if (isBlocking) {
      completableFuture.join
    }
  }

  /** Pauses execution of the JobExecutor.
    *
    * @since 0.1.0
    */
  def pause(): JobExecutor = {
    lock.lock()

    try
      isPaused = true
    finally lock.unlock()

    this
  }

  /** Resumes execution of the JobExecutor.
    *
    * @since 0.1.0
    */
  def resume(): JobExecutor = {
    lock.lock()

    try {
      isPaused = false
      condition.signalAll
    } finally lock.unlock()

    this
  }

  /** Indicates whether or not the [[JobExecutor]] is currently in a paused state.
    *
    * @return true if paused, false otherwise
    * @since 0.1.0
    */
  def paused(): Boolean = isPaused

  /** Cancels execution of the [[JobExecutor]], cancelling execution of any further [[Task]]s.
    *
    * @param reason the reason for cancellation.
    * @since 0.1.0
    */
  def cancel(reason: String): Unit = {
    job.setStatus(JobCanceled(reason))
    completableFuture.cancel(true)
  }

  class RunTask(task: Task) extends Runnable {
    override def run(): Unit = {
      logger.info(s"Running task: $task (paused=$isPaused)")

      lock.lock()

      try {
        while (isPaused) {
          logger.trace("Awaiting lock release.")
          condition.await
          logger.trace("Lock released.")
        }
      } catch {
        case x: InterruptedException => throw x
      } finally {
        lock.unlock()
      }

      job.status match {
        case JobCanceled(x) => {
          logger.info(s"Job canceled with reason $x, stopping all further tasks.")
          task.setStatus(TaskCanceled(x))
          return
        }

        case _ => // Ignore, the job is fine, the only special case is for JobCanceled.
      }

      task.status match {
        case TaskQueued => {
          task.setStatus(TaskRunning)
          task.getStatistics().triggerStart()
          task.task.run()
          task.getStatistics().triggerEnd()
          task.task.onFinished()
          task.setStatus(TaskFinished)
          logger.info(s"Task ${task.name} elapsed time: ${task.getStatistics().getElapsedTime}ms")
        }

        case _ => {
          val failedTaskException = new InvalidTaskStateException(task, task.status, TaskQueued)

          task.setStatus(TaskFailed(failedTaskException))
          job.setStatus(JobFailed(failedTaskException))
        }
      }
    }
  }

  private def runExceptionally(task: Task, f: Throwable): Void = {
    f match {
      case ex: CompletionException => {
        try {
          task.task.onException(ex.getCause)
        } finally {
          task.setStatus(TaskFailed(ex.getCause))
          job.setStatus(JobFailed(ex.getCause))
        }
      }

      case _ => {
        try {
          task.task.onException(f)
        } finally {
          task.setStatus(TaskFailed(f))
          job.setStatus(JobFailed(f))
        }
      }
    }

    throw f
  }

  private def toJavaFunction[A, B](f: Function1[A, B]) = new JavaFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  private def walkSubtasks(dependent: Task, tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      if (!taskMap.contains(task.name)) {
        throw new InvalidTaskDependencyException(dependent.name, task.name)
      }

      if (!taskMap.contains(dependent.name)) {
        val parentFuture: CompletableFuture[Void] = taskMap(task.name)
        val cFuture: CompletableFuture[Void] = if (dependent.async) {
          parentFuture.thenRunAsync(new RunTask(dependent), executorService)
        } else {
          parentFuture.thenRun(new RunTask(dependent))
        }

        taskMap.put(dependent.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
          runExceptionally(dependent, f))))

        logger.debug(s"[${dependent.name}: Queued (async=${dependent.async})] Parent=${task.name} has ${task.dependencies.length} subtasks.")
      }

      if (task.dependencies.nonEmpty) {
        walkSubtasks(task, task.dependencies)
      }
    })
  }

  private def generateExecutionPlan(tasks: Seq[Task]): Unit = {
    tasks
      .filter(task => task.dependencies.isEmpty)
      .foreach(task => {
        if (taskMap.contains(task.name)) {
          throw new DuplicateTaskNameException(task.name)
        }

        logger.debug(s"Task: ${task.name} [ASYNC Root Task]")

        taskMap.put(task.name,
          CompletableFuture.runAsync(new RunTask(task), executorService)
            .exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
              runExceptionally(task, f))))
      })

    tasks
      .filter(task => task.dependencies.nonEmpty)
      .foreach(task => {
        if (taskMap.contains(task.name)) {
          throw new DuplicateTaskNameException(task.name)
        }

        logger.debug(s"Task: ${task.name} task - Walking tree")
        walkSubtasks(task, task.dependencies)
      })

    logger.info(s"Known task map: ${taskMap.keys}")
  }

}

/** Factory object to create a new [[JobExecutor]] object. */
object JobExecutor {
  def apply(job: Job): JobExecutor = new JobExecutor(job)
}

/** An exception indicating that a [[Task]] was in a different state than expected.
  *
  * @param task [[Task]] object.
  * @param status actual [[TaskStatus]].
  * @param expected expected [[TaskStatus]].
  * @since 0.0.1
  */
class InvalidTaskStateException(task: Task,
                                status: TaskStatus,
                                expected: TaskStatus)
  extends Exception(s"InvalidTaskStateException: task ${task.name} set to $status, expected $expected")

/** An exception indicating that an invalid state has occurred while trying to run a job.
  *
  * @param message the exception message.
  * @since 0.1.0
  */
class InvalidJobExecutionStateException(message: String) extends Exception(s"Invalid Job Execution State: ${message}")

/** An exception indicating that a specified task name already exists.
  *
  * @param taskName the name of the task.
  * @since 0.1.0
  */
class DuplicateTaskNameException(taskName: String) extends Exception(s"Task $taskName already exists")

/** An exception indicating that a task depends on another task that has not yet been registered.
  *
  * @param taskName the task being registered
  * @param dependsOn the task the registered task depends on
  * @since 0.1.0
  */
class InvalidTaskDependencyException(taskName: String, dependsOn: String)
  extends Exception(s"Invalid dependency: $taskName depends on $dependsOn which does not yet exist")