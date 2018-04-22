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

/**
  * ExecutionEngine trait
  *
  * An [[ExecutionEngine]] is an implementation that accepts a [[com.scattersphere.core.util.RunnableTask]] and
  * determines where to run the job.
  */
trait ExecutionEngine

object ExecutionEngine {

  def apply(engineType: String): ExecutionEngine =
    engineType.toLowerCase() match {
      case "scala" => new ScalaExecutionEngine
      case _ => throw new IllegalArgumentException(s"Non-supported engine type specified $engineType")
    }

}
