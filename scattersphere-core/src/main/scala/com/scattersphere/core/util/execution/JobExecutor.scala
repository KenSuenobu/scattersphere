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

import java.util.concurrent.CompletableFuture

import com.scattersphere.core.util.{Job, Task}

import scala.collection.mutable

/**
  * JobExecutor class
  *
  * This is the heart of the execution engine.  It takes a [[Job]] object, traverses all of the [[Task]]
  * items defined in it, and creates a DAG.  From this DAG, it determines which tasks can be run asynchronously,
  * and which tasks have dependencies.
  *
  * Any tasks that contain dependencies will block until the task they depend on has completed.  Tasks that are
  * "root" tasks (ie. top level tasks) can be run asynchronously as multiple tasks in multiple threads as they
  * see fit.  The only restriction is based on the Java [[java.util.concurrent.Executor]] object implementation
  * that they choose to use.
  *
  * @param job The [[Job]] containing all of the tasks (and dependencies) to run.
  */
class JobExecutor(job: Job) {

  def walkSubtasks(tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      if (task.getDependencies.nonEmpty) {
        println(s"  `- Task $task has ${task.getDependencies.length} subtasks.  Walking tree.")
        walkSubtasks(task.getDependencies)
      } else {
        println(s"  `- Task $task has no dependencies.")
      }
    })
  }

  def walkTasks(tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      if (task.getDependencies.isEmpty) {
        println(s"Task: $task [Root Task]")
      } else {
        println(s"Task: $task - Walking tree")
        walkSubtasks(task.getDependencies)
      }
    })
  }

  def queue(): Unit = {
    val tasks: Seq[Task] = job.tasks

    walkTasks(tasks)
  }

}