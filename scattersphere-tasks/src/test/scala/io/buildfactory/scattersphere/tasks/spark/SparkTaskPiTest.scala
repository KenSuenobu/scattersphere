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

package io.buildfactory.scattersphere.tasks.spark

import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger
import io.buildfactory.scattersphere.core.util.spark.SparkCache
import io.buildfactory.scattersphere.core.util.{JobBuilder, TaskBuilder}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import scala.math.random
import scala.util.Properties

class SparkTaskPiTest extends FlatSpec with Matchers with SimpleLogger {

  SparkCache.save("sparkPiTestCache", new SparkConf()
    .setMaster(Properties.envOrElse("SPARK_MASTER", "local[*]"))
    .setAppName("local pi test")
    .setJars(Array("target/scattersphere-tasks-0.2.2-tests.jar"))
    .set("spark.ui.enabled", "false"))

  "Spark task pi test" should "calculate Pi in the form of a task" in {
    val piTask: SparkTask = new SparkTask("sparkPiTestCache") {
      override def run(): Unit = {
        val slices = Runtime.getRuntime().availableProcessors()
        val n = math.min(100000L * slices, Int.MaxValue).toInt
        val count = getContext()
          .parallelize(1 until n, slices)
          .map { _ =>
            val x = random * 2 - 1
            val y = random * 2 - 1

            if (x * x + y * y <= 1) 1 else 0
          }
          .reduce(_ + _)

        val pi = 4.0 * count / (n - 1)

        assert(pi >= 3.0 && pi <= 3.2)

        println(s"Pi calculated to approximately ${pi}")
      }
    }

    var sTask = TaskBuilder("Pi Task")
      .withTask(piTask)
      .build()
    var sJob = JobBuilder()
      .withTasks(sTask)
      .build()
    var jExec: JobExecutor = JobExecutor(sJob)

    jExec.queue().run()
  }

}
