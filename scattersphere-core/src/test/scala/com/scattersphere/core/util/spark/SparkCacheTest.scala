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

package com.scattersphere.core.util.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.math.random
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Properties

/** Spark Cache Test code.  Utilizes some code from the SparkPi test from the official Spark source code.
  * Reference: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
  *
  * @since 0.2.0
  */
class SparkCacheTest extends FlatSpec with Matchers with LazyLogging {

  "Spark Cache" should "calculate Pi quickly in a local[*] context" in {
    SparkCache.save("test", new SparkConf()
      .setMaster(Properties.envOrElse("SPARK_MASTER", "local[*]"))
      .setAppName("local pi test")
      .setJars(Array("target/scattersphere-tasks-0.2.0.jar",
                     "../scattersphere-core/target/scattersphere-core-0.2.0.jar"))
      .set("spark.ui.enabled", "false"))
    val spark: SparkSession = SparkCache.getSession("test")
    val sContext: SparkContext = spark.sparkContext

    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = sContext.parallelize(1 until n, slices).map { _ =>
      val x = random * 2 - 1
      val y = random * 2 - 1

      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)

    val pi = 4.0 * count / (n - 1)

    assert(pi >= 3.0 && pi <= 3.2)

    spark.stop
  }

  it should "throw a NullPointerException for a missing key or missing SparkConf" in {
    assertThrows[NullPointerException] {
      SparkCache.save(null, new SparkConf())
    }

    assertThrows[NullPointerException] {
      SparkCache.save("test1234", null)
    }
  }

  it should "throw a NullPointerException for a missing key when creating a SparkSession" in {
    assertThrows[NullPointerException] {
      SparkCache.getSession("nonexistent entry")
    }
  }

}
