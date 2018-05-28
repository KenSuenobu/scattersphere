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

import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletionException, _}
import java.util.function.{Function => JavaFunction}

import com.scattersphere.core.util._
import com.typesafe.scalalogging.LazyLogging

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
class JobExecutor(job: Job) extends LazyLogging {

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

  private var completableFuture: CompletableFuture[Void] = _

  /** Walks the tree of all tasks for this job, creating an execution DAG.  Since the top-level tasks run using an
    * asynchronous CompletableFuture, it's possible that the tasks will start while the DAG is being generated.
    * This should not affect how the tasks run, however, it may affect synchronization in your top-level application,
    * should you depend on timing or anything of that sort.
    *
    * @return this object
    */
  def queue(): JobExecutor = {
    val tasks: Seq[Task] = job.tasks

    generateExecutionPlan(tasks)

    completableFuture = CompletableFuture
      .allOf(taskMap.values.toSeq: _*)
      .whenComplete((_, _) => {
        job.status match {
          case JobRunning => job.setStatus(JobFinished)
          case _ => // Do nothing; keep state stored
        }

        executorService.shutdown
        logger.trace("Execution service shut down.")
      })

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

  /** Executes the DAG. When set to non-blocking, this will start the DAG and will return immediately. */
  def run(): Unit = {
    job.setStatus(JobRunning)
    resume()

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

  private def runTask(task: Task): Unit = {
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
      if (!taskMap.contains(dependent.name)) {
        val parentFuture: CompletableFuture[Void] = taskMap(task.name)
        val cFuture: CompletableFuture[Void] = dependent.async match {
          case true => parentFuture.thenRunAsync(() => runTask(dependent), executorService)
          case false => parentFuture.thenRun(() => runTask(dependent))
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
        logger.debug(s"Task: ${task.name} [ASYNC Root Task]")

        taskMap.put(task.name,
          CompletableFuture.runAsync(() => runTask(task), executorService)
            .exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) =>
              runExceptionally(task, f))))
      })

    tasks
      .filter(task => task.dependencies.nonEmpty)
      .foreach(task => {
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