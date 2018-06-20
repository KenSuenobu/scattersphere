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

package io.buildfactory.scattersphere.core.util

import java.util.concurrent.atomic.AtomicInteger

import io.buildfactory.scattersphere.core.util.JobBuilder._

/** A collection of [[Task]]s.
  *
  * [[Job]]s are run using the [[io.buildfactory.scattersphere.core.util.execution.JobExecutor]] class, which in turn
  * controls the execution of the [[Task]]s within each [[Job]].
  *
  * A [[Job]] contains a name, a Seq of [[Task]]s, and a status.  When initialized, the [[JobStatus]]
  * is set to [[JobQueued]], indicating that a [[Job]] is ready to run, but is in a dormant state.
  *
  * ==Example==
  *
  * Creating a new [[Job]]:
  * {{{
  *   val job: Job = new Job(name = "my job", tasks = Seq(task1, task2, task3))
  * }}}
  *
  * @constructor creates a new object with a name and sequence of [[Task]]s, setting the [[JobStatus]] to [[JobQueued]]
  * @param id id of the task.
  * @param name name of the job.
  * @param tasks The `Seq` of [[Task]]s associated with the job.
  * @since 0.0.1
  */
case class Job(id: Int, name: String, tasks: Seq[Task]) {

  private var jobStatus: JobStatus = JobQueued
  private val jobStatistics: Statistics = new Statistics

  /** Sets the [[JobStatus]] for this job.
    *
    * @param status [[JobStatus]] to assign.
    */
  def setStatus(status: JobStatus): Unit = jobStatus = status

  /** Retrieves the current [[JobStatus]]
    *
    * @return [[JobStatus]] for this job.
    */
  def status: JobStatus = jobStatus

  def getStatistics(): Statistics = jobStatistics

  override def toString = s"Job{name=$name,jobStatus=$jobStatus,statistics=$jobStatistics,tasks=$tasks}"

}

/** A builder class that allows for functional construction of a [[Job]].
  *
  * The JobBuilder allows for chained functions to be used to functionally create a Job.
  *
  * ==Example==
  *
  * {{{
  *   val job: Job = JobBuilder("my job")
  *     .addTasks(task1, task2, task3)
  *     .build()
  * }}}
  *
  * Adding a single task at a time can be done using the singular `addTask(task)` method.
  *
  * ==Nameless Example==
  *
  * Given the following example:
  *
  * {{{
  *   val task: Task = TaskBuilder("My Task")
  *     .withTask(new MyTask())
  *     .build()
  *   val job: Job = JobBuilder()
  *     .addTasks(task)
  *     .build()
  * }}}
  *
  * The [[Job]] here would be implied to have no name.  If this is the case, the name will be
  * extracted from the first task in the task list, with " Job" tacked on the end of it.  So, in
  * this case, the name would become "My Task Job".
  *
  * @since 0.0.1
  */
class JobBuilder {

  private var tasks: Seq[Task] = Seq()
  private var jobName: String = ""

  /** Defines the name of the job.
    *
    * @param name name of the job.
    * @return this object for continued building.
    */
  def withName(name: String): JobBuilder = {
    jobName = name
    this
  }

  /** Adds a series of [[Task]]s.
    *
    * @param taskList series of [[Task]]s to add.
    * @return this object for continued building.
    * @since 0.0.2
    */
  def withTasks(taskList: Task*): JobBuilder = {
    taskList.foreach(task => tasks = tasks :+ task)
    this
  }

  /** Builds a new [[Job]] object given the supplied parameters.
    *
    * @return a new [[Job]] object.
    */
  def build(): Job = {
    if (tasks.size == 0) {
      throw new IllegalArgumentException("Missing task list.")
    }

    Job(JOB_ID_GENERATOR.incrementAndGet(), if (jobName.length == 0) s"${tasks(0).name} Job".trim else jobName, tasks)
  }

}

/** Factory class with convenience method to create a new [[JobBuilder]] on demand. */
object JobBuilder {
  private val JOB_ID_GENERATOR: AtomicInteger = new AtomicInteger(0)

  /** Creates a new [[JobBuilder]] object. */
  def apply(): JobBuilder = new JobBuilder()

  /** Creates a new [[JobBuilder]], applying the job name. */
  def apply(name: String): JobBuilder = new JobBuilder().withName(name)
}

/** This is the root class that all status values should inherit.
  *
  * @param t the optional Throwable object associated with the status.
  * @param reason the reason for a cancellation.
  * @since 0.0.1
  */
sealed abstract class JobStatus(t: Throwable = null, reason: String = null)

/** Indicates that a job is queued and dormant, meaning, it is initialized, but has not been run.
  *
  * @since 0.0.1
  */
final case object JobQueued extends JobStatus

/** Indicates that a job is currently running.
  *
  * @since 0.0.1
  */
final case object JobRunning extends JobStatus

/** Indicates that a job has completed without errors.
  *
  * @since 0.0.1
  */
final case object JobFinished extends JobStatus

/** Indicates that a job failed at some point with an error.  To find the [[Task]] that failed, you
  * will need to walk the sequence of [[Task]] objects that are part of the [[Job]] that failed.  A
  * failed task will have a status of [[TaskFailed]], along with the associated Throwable.
  *
  * @constructor create a new status with the associated `Throwable` that caused the error.
  * @param t `Throwable` that caused the [[Job]] to fail.
  * @since 0.0.1
  */
final case class JobFailed(t: Throwable) extends JobStatus(t = t)

/** Indicates that a job failed due to a cancellation.
  *
  * @param reason the reason for a cancellation.
  */
final case class JobCanceled(reason: String) extends JobStatus(reason = reason)