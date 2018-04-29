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

import scala.collection.mutable.ListBuffer

/**
  * TaskDesc
  *
  * Describes a task and its [[Runnable]] runnable function.  Allows for dependencies to be added.
  *
  * @param name The name of the task.
  * @param task The [[RunnableTask]] to run.
  * @param async When true, the task will run asynchronously after the dependent task completes; false will run
  *              synchronously after the dependent task(s) complete.
  */
case class Task(name: String, task: RunnableTask, async: Boolean = false) {

  lazy private val dependencies: ListBuffer[Task] = new ListBuffer[Task]

  /**
    * Adds a dependent task that is required to complete before this task starts.  This can be multiple tasks, not
    * just a single task.  If multiple tasks are set here, all of the tasks that have been identified must complete
    * before this tasks starts.
    *
    * @param task The task to add a dependency against.
    */
  def addDependency(task: Task): Unit = {
    if (task.equals(this)) {
      throw new IllegalArgumentException("Unable to add task: task is self")
    }

    dependencies += task
  }

  /**
    * Retrieves all of the dependencies for this task.
    *
    * @return [[Seq]] containing [[Task]] dependency list.
    */
  def getDependencies: Seq[Task] = dependencies

  override def toString: String = s"Task{name=$name,dependencies=${dependencies.length}}"

}
