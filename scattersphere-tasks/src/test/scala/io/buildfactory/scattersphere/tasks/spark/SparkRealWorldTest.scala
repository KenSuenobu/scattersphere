/*
 *    _____            __  __                       __
 *   / ___/_________ _/ /_/ /____  ______________  / /_  ___  ________
 *   \__ \/ ___/ __ `/ __/ __/ _ \/ ___/ ___/ __ \/ __ \/ _ \/ ___/ _ \
 *  ___/ / /__/ /_/ / /_/ /_/  __/ /  (__  ) /_/ / / / /  __/ /  /  __/
 * /____/\___/\__,_/\__/\__/\___/_/  /____/ .___/_/ /_/\___/_/   \___/
 *                                       /_/
 *
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

import java.io.PrintWriter
import java.util.regex.{Matcher, Pattern}

import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger
import io.buildfactory.scattersphere.core.util.spark.SparkCache
import io.buildfactory.scattersphere.core.util.{JobBuilder, TaskBuilder}
import io.buildfactory.scattersphere.tasks.spark.SparkRealWorldTest._
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Properties

class SparkRealWorldTest extends FlatSpec with Matchers with SimpleLogger {

  SparkCache.save("realWorldTest", new SparkConf()
    .setMaster(Properties.envOrElse("SPARK_MASTER", "local[*]"))
    .setAppName("local pi test")
    .setJars(Array("target/scattersphere-tasks-0.2.2-tests.jar"))
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

    /**
      * Due to memory restrictions, this SparkTask will only take the first 10 URLs and parse them for words.
      */
    val wordCounterTask: SparkTask = new SparkTask("realWorldTest") {
      def mapWordsFromUrl(iter: Iterator[String]): Iterator[(String, Map[String, Int])] = {
        var returnList: ArrayBuffer[(String, Map[String, Int])] = mutable.ArrayBuffer()

        while(iter.hasNext) {
          val url = iter.next()

          try {
            val words = Source.fromURL(url).mkString.toLowerCase()
            val wordsMap: Map[String, Int] = words
              .replaceAll("[\r\n]", "")
              .replaceAll("\\<.*?\\>", "")
              .replaceAll("[^a-zA-Z0-9 ]", "")
              .split(" ")
              .map(_.toLowerCase)
              .groupBy(identity)
              .mapValues(_.size)
              .map(identity)      // <- This is required for serialization in Spark

            logger.info(s"Parsed words: URL=${url} count=${wordsMap.size}")

            returnList += ((url, wordsMap))
          } catch {
            case _: Throwable => println(s"Ignoring exception for URL ${url}")
          }
        }

        returnList.iterator
      }

      override def run(): Unit = {
        val results = getContext()
          .parallelize(uniqueUrls.take(MAX_TAKE_URLS))
          .repartition(NUM_PROCESSORS)
          .mapPartitions(mapWordsFromUrl)
          .foreach(entry => {
            val url = entry._1
            val urlMap = entry._2
            val outFile = s"/tmp/${System.nanoTime()}-${urlMap.size}-found-words"

            logger.info(s"Writing results: url=${url} file=${outFile}")

            // This writes to the outfile, sorting the number of occurrences.
            new PrintWriter(outFile) {
              for ((word, counter) <- ListMap(urlMap.toSeq.sortWith(_._2 > _._2): _*)) {
                write(s"$word\t$counter\n")
              }
              close()
            }
          })
      }
    }

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

object SparkRealWorldTest {
  private val NUM_PROCESSORS = Runtime.getRuntime().availableProcessors()
  private val MAX_TAKE_URLS = 10
}