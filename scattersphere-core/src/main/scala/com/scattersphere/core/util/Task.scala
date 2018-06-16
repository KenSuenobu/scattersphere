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
import com.scattersphere.core.util.TaskBuilder._

/** A unit of work in the form of a [[RunnableTask]].
  *
  * [[Task]]s are run from within [[Job]] objects, which contain a collection of one or more [[Task]] objects.  A
  * task is simply a unit of work that will be performed in a [[Job]].
  *
  * Tasks can be run synchronously, or asynchronously.  This means that when a series of [[Task]]s are created,
  * the next [[Task]] must wait until this [[Task]]'s unit of work completes.  When set asynchronously, the next
  * task does not need to wait for the dependent task to terminate.
  *
  * General tips for choosing between synchronous and asynchronous tasks:
  *
  * ==Synchronous Tasks==
  *
  *   - Single unit of work that blocks until complete
  *   - Requires that the unit of work completes until the next step can run
  *   - Can take results from multiple other tasks, and combine them into one after complete
  *   - Can be fire-and-forget
  *
  * An example of a synchronous task could be this:
  * {{{
  *
  *   [Task 1] -> [Task 2] -> [Task 3] -> ...
  *
  * }}}
  *
  * This above example means that `Task 1` will run.  Once `Task 1` terminates, `Task 2` will begin.  Then
  * `Task 3`, and so on.  Tasks are synchronous by default, as asynchronous tasks are a little more difficult
  * to logically coordinate.
  *
  * ==Asynchronous Tasks==
  *
  *   - Can be run in the background (like a fetch, a database lookup, etc.)
  *   - Can be more difficult to coordinate work units (behavior undefined due to async running)
  *   - Can also be fire-and-forget
  *
  * An example of asynchronous tasks could be this:
  * {{{
  *
  *   [Async Task 1] --\
  *                   [Sync Task 3] -> ...
  *   [Async Task 2] --/
  *
  * }}}
  *
  * Asynchronous `Task 1` and `Task 2` will run at the same time.  `Sync Task 3` is a synchronous task that will
  * run only when `Task 1` and `Task 2` have both completed.
  *
  * Generally speaking, any tasks that split off into two or more units of work can be defined as ''asynchronous''.
  * To sync up the DAG properly, the units that depend on those asynchronous units of work should be
  * ''synchronous'', so that the work can be coordinated and synchronized properly.
  *
  * We leave this behavior up to you, though.  It doesn't necessarily matter which way you set up the tasks and
  * the DAG, but it is up to you to synchronize the entire process properly, as large DAGs of tasks can be
  * difficult to debug.
  *
  * @param id the id of the task
  * @param name name of the task
  * @param task [[RunnableTask]] class unit of work
  * @param dependencies tasks that this task depends on before running
  * @param async true for asynchronous, false otherwise
  * @since 0.0.1
  */
case class Task(id: Int, name: String, task: RunnableTask, dependencies: Seq[Task], async: Boolean = false) {

  private var taskStatus: TaskStatus = TaskQueued
  private val taskStatistics: TaskStatistics = new TaskStatistics

  /** Sets the status for this task.
    *
    * @param status [[TaskStatus]] to set
    */
  def setStatus(status: TaskStatus) = {
    task.onStatusChange(taskStatus, status)
    taskStatus = status
  }

  /** The current task status.
    *
    * @return [[TaskStatus]].
    */
  def status(): TaskStatus = taskStatus

  /** The current task statistics.
    *
    * @return [[TaskStatistics]] object.
    */
  def getStatistics(): TaskStatistics = taskStatistics

  override def toString: String = s"Task{id=$id,name=$name,status=$taskStatus," +
    s"dependencies=${dependencies.length},async=$async}"

}

/** Factory class implementation of the [[Task]] adding the ability to create a [[Task]] by supplying the method
  * body instead of having to create a [[Task]] object by hand.
  *
  * ==Example==
  * {{{
  *   val task1: Task = Task {
  *     Thread.sleep(500)
  *     logger.info("Slept 500 ms.")
  *     Thread.sleep(500)
  *     logger.info("Slept an additional 500 ms.")
  *   val job1: Job = JobBuilder()
  *     .withName("Simple sleeper job")
  *     .withTasks(task1)
  *     .build()
  *   val jobExec: JobExecutor = new JobExecutor(job1)
  *
  *   job1.queue().run()
  * }}}
  *
  * Super-convenient way to create a synchronous [[Task]] without having to do a bunch of class definitions.
  *
  * Asynchronous tasks can be built the same way with the Task.async companion object.
  *
  * @since 0.0.3
  */
object Task {

  /** Generate a synchronous [[Task]] using the body of the task as the runnable code.
    *
    * @param name the name of the task
    * @param a function code to run
    * @return [[Task]] with the closure wrapped in a [[RunnableTask]], with no name and no dependencies.
    */
  def apply(name: String)(block: => Unit): Task = evaluate(name, block)

  /** Generate an asynchronous [[Task]] using the body of the task as the runnable code.
    *
    * @param name the name of the task
    * @param a function code to run
    * @return [[Task]] with the closure wrapped in a [[RunnableTask]], with no name and no dependencies.
    */
  def async(name: String)(block: => Unit): Task = evaluate(name, block, true)

  private def evaluate(name: String, block: => Unit, async: Boolean = false) =
    Task(TaskBuilder.TASK_ID_GENERATOR.incrementAndGet(), name, new RunnableTask {
      override def run(): Unit = block
    }, Seq(), async)

}

/** Statistics class that stores information about when a [[Task]] starts and ends, including the total elapsed time.
  *
  * @since 0.2.0
  */
class TaskStatistics {
  private var timeStarted: Long = 0
  private var timeEnded: Long = 0

  /** Triggers the start of the task. */
  def triggerStart(): Unit = timeStarted = System.currentTimeMillis()

  /** Triggers the end of the task. */
  def triggerEnd(): Unit = timeEnded = System.currentTimeMillis()

  /** Retrieves the total runtime for the task.
    *
    * @return Runtime in milliseconds.
    */
  def getRuntime(): Long = (timeEnded - timeStarted)

  override def toString: String = s"TaskStatistics{timeStarted=$timeStarted,timeEnded=$timeEnded}"
}

/** A builder class that allows for functional construction of a [[Task]].
  *
  * The `TaskBuilder` allows for chained functions to be used to functionally create a [[Task]].
  *
  * ==Example==
  * {{{
  *   val task1: Task = new TaskBuilder()
  *     .withName("T1")
  *     .withTask(new SyncTask)
  *     .build()
  *   val task2: Task = new TaskBuilder()
  *     .withName("T1-1")
  *     .withTask(new AsyncTask)
  *     .async()
  *     .dependsOn(task1)
  *     .build()
  *   val task3: Task = new TaskBuilder()
  *     .withName("T1-2")
  *     .withTask(new AsyncTask)
  *     .async()
  *     .dependsOn(task1)
  *     .build()
  * }}}
  *
  * In this example, `Task 1` is created, and will run synchronously.  Once done, `task2` and `task3` will
  * kick off and run asynchronously.
  *
  * This DAG looks something like this:
  *
  * {{{
  *              /---> [Task 2]
  *   [Task 1] -<
  *              \---> [Task 3]
  * }}}
  *
  * @since 0.0.1
  */
class TaskBuilder {

  private var taskName: String = ""
  private var runnableTask: RunnableTask = _
  private var dependencies: Seq[Task] = Seq()
  private var taskAsync: Boolean = false

  /** Defines the name of the task.
    *
    * @param name name of the task
    * @return this object
    */
  def withName(name: String): TaskBuilder = {
    taskName = name
    this
  }

  /** Defines the task to run.
    *
    * @param task [[RunnableTask]] object.
    * @return this object
    */
  def withTask(task: RunnableTask): TaskBuilder = {
    runnableTask = task
    this
  }

  /** Defines a dependent task.
    *
    * @param tasks a list of tasks to depend on, comma separated
    * @return this object
    * @since 0.0.2
    */
  def dependsOn(tasks: Task*): TaskBuilder = {
    tasks.foreach(task => dependencies = dependencies :+ task)
    this
  }

  /** Sets this task to run asynchronously.
    *
    * @return this object
    */
  def async(): TaskBuilder = {
    taskAsync = true
    this
  }

  /** Builds a new [[Task]] defined by the builder parameters specified.
    *
    * @return [[Task]] object.
    */
  def build(): Task = {
    if (taskName.length == 0) {
      throw new IllegalArgumentException("Missing task name")
    }

    Task(TASK_ID_GENERATOR.incrementAndGet(), taskName, runnableTask, dependencies, taskAsync)
  }

}

/** Factory class with convenience method to create a new [[TaskBuilder]] on demand. */
object TaskBuilder {
  val TASK_ID_GENERATOR: AtomicInteger = new AtomicInteger(0)

  /** Creates a new TaskBuilder with no options. */
  def apply(): TaskBuilder = new TaskBuilder()

  /** Creates a new TaskBuilder, building a task by name. */
  def apply(name: String): TaskBuilder = new TaskBuilder().withName(name)
}

/** This is the root class that all status values should inherit.
  *
  * @param t `Throwable` that may have occurred.
  * @param reason `String` that contains a reason for a cancellation.
  * @since 0.0.1
  */
sealed abstract class TaskStatus(t: Throwable = null, reason: String = null)

/** Indicates that a [[Task]] is queued and dormant.
  *
  * @since 0.0.1
  */
final case object TaskQueued extends TaskStatus

/** Indicates that a [[Task]] is running.
  *
  * @since 0.0.1
  */
final case object TaskRunning extends TaskStatus

/** Indicates that a [[Task]] has completed.
  *
  * @since 0.0.1
  */
final case object TaskFinished extends TaskStatus

/** Indicates that a [[Task]] failed due to an `Exception`.
  *
  * @param t `Throwable` that caused the failure.
  * @since 0.0.1
  */
final case class TaskFailed(t: Throwable) extends TaskStatus(t = t)

/** Indicates that a [[Task]] failed due to a cancellation.
  *
  * @param reason `String` that contains a reason for a cancellation.
  * @since 0.1.0
  */
final case class TaskCanceled(reason: String) extends TaskStatus(reason = reason)