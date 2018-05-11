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

case class Task(name: String, task: RunnableTask, dependencies: Seq[Task], async: Boolean = false) {

  private var taskStatus: TaskStatus = TaskQueued

  /**
    * Sets the status for this task.
    *
    * @param status The [[TaskStatus]] to set
    */
  def setStatus(status: TaskStatus) = taskStatus = status

  /**
    * Returns the current task status.
    *
    * @return [[TaskStatus]] containing the task status.
    */
  def status(): TaskStatus = taskStatus

  override def toString: String = s"Task{name=$name,status=$taskStatus,dependencies=${dependencies.length}}"

}

class TaskBuilder {

  private var taskName: String = _
  private var runnableTask: RunnableTask = _
  private var dependencies: Seq[Task] = Seq()
  private var taskAsync: Boolean = false

  def withName(name: String): TaskBuilder = {
    taskName = name
    this
  }

  def withTask(task: RunnableTask): TaskBuilder = {
    runnableTask = task
    this
  }

  def dependsOn(task: Task): TaskBuilder = {
    dependencies = dependencies :+ task
    this
  }

  def async(): TaskBuilder = {
    taskAsync = true
    this
  }

  def build(): Task = new Task(taskName, runnableTask, dependencies, taskAsync)

}

/**
  * This is the root class that all status values should inherit.
  */
sealed abstract class TaskStatus(t: Throwable = null)

/**
  * This indicates that a task is queued.
  */
final case object TaskQueued extends TaskStatus

/**
  * This indicates that a task is running.
  */
final case object TaskRunning extends TaskStatus

/**
  * This indicates that a task has completed.
  */
final case object TaskFinished extends TaskStatus

/**
  * This indicates that a task was canceled.
  */
final case class TaskFailed(t: Throwable) extends TaskStatus(t)

