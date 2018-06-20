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
package io.buildfactory.scattersphere.core

import io.buildfactory.scattersphere.core.util._

/** Provides utility classes for dealing with creation of [[Job]]s and [[Task]]s.
  *
  *   - A [[Task]] is a unit of work.
  *   - A [[Job]] is a collections of [[Task]]s.
  *
  * ==Overview==
  *
  * A [[Task]] contains a unit of work that is contained in a wrapped `Runnable` object called the [[RunnableTask]].
  * The key difference between the two is the [[RunnableTask]] adds the ability to detect when the task finishes,
  * or when an `Exception` occurs.
  *
  * [[Task]]s and [[Job]]s are created using the [[TaskBuilder]] and [[JobBuilder]] classes, respectively.
  *
  * ==Examples==
  *
  * An example of creating a [[Task]]:
  * {{{
  *   val task: Task = new TaskBuilder()
  *     .withName("My task")
  *     .withTask(new Runnable { } ... with RunnableTask)
  *     .build()
  * }}}
  *
  * And, an example of creating a [[Job]]:
  * {{{
  *   val job: Job = new JobBuilder()
  *     .withName("My job")
  *     .addTasks(task1, task2, task3, ...)
  *     .build()
  * }}}
  *
  * Execution of [[Job]]s is handled in the [[io.buildfactory.scattersphere.core.util.execution]] package.
  *
  * @since 0.0.1
  */
package object util { }
