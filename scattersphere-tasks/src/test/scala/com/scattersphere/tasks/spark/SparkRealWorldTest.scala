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

package com.scattersphere.tasks.spark

import java.io.PrintWriter
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.regex.{Matcher, Pattern}

import com.scattersphere.core.util.{JobBuilder, TaskBuilder}
import com.scattersphere.core.util.execution.JobExecutor
import com.scattersphere.core.util.spark.SparkCache
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source

class SparkRealWorldTest extends FlatSpec with Matchers with LazyLogging {

  SparkCache.save("realWorldTest", new SparkConf()
    .setMaster("local[*]")
    .setAppName("local pi test")
    .set("spark.ui.enabled", "false"))

  "Spark Real World Test" should "be able to run in Spark" in {
    var urlData: Array[(String, String)] = null
    var uniqueUrls: Seq[String] = null

    val fetchTask: SparkTask = new SparkTask("realWorldTest") {
      override def run(): Unit = {
        val urls = Array("https://www.scala-lang.org/",
          "https://www.rust-lang.org/en-US/documentation.html")
        urlData = getContext()
          .parallelize(urls)
          .map(url => url.toLowerCase() -> Source.fromURL(url).mkString.toLowerCase())
          .collect()
      }
    }

    val uniqueUrlStripTask: SparkTask = new SparkTask("realWorldTest") {
      override def run(): Unit = {
        val collectedUrls = getContext()
          .parallelize(urlData)
          .map(a => {
            val data: String = a._2
            val urlPattern: Pattern = Pattern.compile(
              "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
              Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL)
            val matcher: Matcher = urlPattern.matcher(data)
            var foundUrls: mutable.SortedSet[String] = mutable.SortedSet[String]()

            while (matcher.find()) {
              val start: Int = matcher.start(1)
              val end: Int = matcher.end()
              val matchedUrl = data.substring(start, end)

              foundUrls += matchedUrl
            }

            foundUrls.toSet
          })
          .collect()

        uniqueUrls = collectedUrls.flatten.toSeq
      }
    }

    val wordCounterTask: SparkTask = new SparkTask("realWorldTest") {
      override def run(): Unit = {
//        var counterMap: ConcurrentHashMap[String, Integer] = new ConcurrentHashMap()
//
//        getContext()
//          .parallelize(uniqueUrls)
//          .foreachPartition(urls => {
//            urls.foreach(url => {
//              try {
//                val words = Source.fromURL(url).mkString.toLowerCase()
//                val wordsMap: Map[String, Int] = words
//                  .replaceAll("[\r\n]", "")
//                  .replaceAll("\\<.*?\\>", "")
//                  .replaceAll("[^a-zA-Z0-9 ]", "")
//                  .split(" ")
//                  .map(_.toLowerCase)
//                  .groupBy(identity)
//                  .mapValues(_.size)
//
//                for ((word, counter) <- ListMap(wordsMap.toSeq.sortWith(_._2 > _._2): _*)) {
//                  if (counterMap.get(word) != null) {
//                    counterMap.put(word, counterMap.get(word) + counter)
//                  } else {
//                    counterMap.put(word, counter)
//                  }
//                }
//              } catch {
//                case _ => println(s"Ignoring exception for URL ${url}")
//              }
//            })
//          })
      }
    }

//        val slices = 2
//        val n = math.min(100000L * slices, Int.MaxValue).toInt
//        val count = getContext().parallelize(1 until n, slices).map { _ =>
//          val x = random * 2 - 1
//          val y = random * 2 - 1
//
//          if (x * x + y * y <= 1) 1 else 0
//        }.reduce(_ + _)
//
//        val pi = 4.0 * count / (n - 1)
//
//        assert(pi >= 3.0 && pi <= 3.2)
//
//        println(s"Pi calculated to approximately ${pi}")

    val sTask1 = TaskBuilder("Spark URL Fetch Task")
      .withTask(fetchTask)
      .build()
    val sTask2 = TaskBuilder("Spark URL Strip Task")
      .withTask(uniqueUrlStripTask)
      .dependsOn(sTask1)
      .build()
    val sTask3 = TaskBuilder("Spark Word Counter Task")
      .withTask(wordCounterTask)
      .dependsOn(sTask2)
      .build()
    val sJob = JobBuilder()
      .withTasks(sTask1, sTask2, sTask3)
      .build()
    val jExec: JobExecutor = JobExecutor(sJob)

    jExec.queue().run()
  }

}