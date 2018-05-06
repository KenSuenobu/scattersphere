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

/**
  * Job
  *
  * This class describes a job.  A job is simply a series of tasks.
  *
  * @param name The name of the job.
  * @param tasks The Seq of [[Task]]s associated with the job.
  */
case class Job(name: String, tasks: Seq[Task]) {

  private var jobStatus: JobStatus = JobQueued

  def setStatus(status: JobStatus): Unit = jobStatus = status

  def status(): JobStatus = jobStatus

  override def toString = s"Job{name=$name,jobStatus=$jobStatus,tasks=$tasks}"

}

/**
  * This is the root class that all status values should inherit.
  */
sealed abstract class JobStatus(t: Throwable = null)

/**
  * This indicates that a job is queued but not running.
  */
final case object JobQueued extends JobStatus

/**
  * This indicates that a job is running.
  */
final case object JobRunning extends JobStatus

/**
  * This indicates that a job has completed.
  */
final case object JobFinished extends JobStatus

/**
  * This indicates that a job was canceled.
  */
final case class JobFailed(t: Throwable) extends JobStatus(t)

