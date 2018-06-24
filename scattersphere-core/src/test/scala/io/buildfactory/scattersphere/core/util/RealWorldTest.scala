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

package io.buildfactory.scattersphere.core.util

import java.io.PrintWriter
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.regex.{Matcher, Pattern}

import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source

/**
  * This will eventually contain a series of real world tests that provide a way to show real use cases in
  * Scattersphere.
  */
class RealWorldTest extends FlatSpec with Matchers with SimpleLogger {

  /**
    * This first example walks a series of URLs and:
    *   - loads in the data into a concurrent map.
    *   - strips the URL data of any additional web site URLs.
    *   - counts the number of words in the URL body, sorts the results by occurrence.
    *
    * The DAG looks like this:
    * {{{
    *            /---> [URL finder]
    *   [URL fetch]
    *            \---> [Word counter]
    * }}}
    *
    * Whereas the URL Fetch task runs as a synchronous task before firing off the URL finder and word counters
    * in parallel as two separate asynchronous tasks.
    */
  "real world test" should "fetch data from a series of URLs, parse the data, and generate analytics" in {
    val urls = Array("https://www.scala-lang.org/",
      "https://www.rust-lang.org/en-US/documentation.html")
    var webData: ConcurrentMap[String, String] = new ConcurrentHashMap[String, String]()

    class StripFetchedDataRunnable(url: String, count: Int) extends Runnable {
      override def run(): Unit = {
        val data: String = webData.get(url.toLowerCase())
        val urlPattern: Pattern = Pattern.compile(
          "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
            + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
            + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
          Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL)
        val matcher: Matcher = urlPattern.matcher(data)
        var foundUrls: mutable.SortedSet[String] = mutable.SortedSet[String]()

        while(matcher.find()) {
          val start: Int = matcher.start(1)
          val end: Int = matcher.end()
          val matchedUrl = data.substring(start, end)

          foundUrls += matchedUrl
        }

        println("Writing unique URLs to: " + s"/tmp/${count}-found-urls")
        new PrintWriter(s"/tmp/${count}-found-urls") {
          foundUrls.foreach(x => write(s"$x\n"))
          close()
        }
      }
    }

    class CountWordsRunnable(url: String, count: Int) extends Runnable {
      override def run(): Unit = {
        val wordsMap: Map[String, Int] = webData.get(url.toLowerCase())
          .replaceAll("[\r\n]", "")
          .replaceAll("\\<.*?\\>", "")
          .replaceAll("[^a-zA-Z0-9 ]", "")
          .split(" ")
          .map(_.toLowerCase)
          .groupBy(identity)
          .mapValues(_.size)

        println("Writing word counts to: " + s"/tmp/${count}-found-words")
        new PrintWriter(s"/tmp/${count}-found-words") {
          for ((word, counter) <- ListMap(wordsMap.toSeq.sortWith(_._2 > _._2): _*)) {
            write(s"$word\t$counter\n")
          }
          close()
        }
      }
    }

    for((url, counter) <- urls.zipWithIndex) {
      val fetcherTask: Task = Task("Fetcher Task") {
        logger.debug(s"Fetching URL $url")
        val data: String = Source.fromURL(url).mkString
        webData.put(url.toLowerCase(), data.toLowerCase())
        logger.debug(s"Fetch of $url complete: ${data.length} bytes")
      }
      val stripDataRunnableTask: RunnableTask = RunnableTask(new StripFetchedDataRunnable(url, counter))
      val wordsCountRunnableTask: RunnableTask = RunnableTask(new CountWordsRunnable(url, counter))
      val stripDataTask: Task = TaskBuilder("Strip Data Task")
        .withTask(stripDataRunnableTask)
        .dependsOn(fetcherTask)
        .async()
        .build()
      val wordsCountTask: Task = TaskBuilder("Words Count Task")
        .withTask(wordsCountRunnableTask)
        .dependsOn(fetcherTask)
        .async()
        .build()
      val urlTestJob: Job = JobBuilder()
        .withTasks(fetcherTask, stripDataTask, wordsCountTask)
        .build()
      val jobExec: JobExecutor = JobExecutor(urlTestJob)

      assert(urlTestJob.id > 0)
      assert(stripDataTask.id > 0)
      assert(wordsCountTask.id > 0)
      println(s"Running job for URL: $url")

      val startTime: Long = System.currentTimeMillis()
      jobExec.queue().run()
      val elapsed: Long = System.currentTimeMillis() - startTime

      println(s"Time to retrieve data from $url: $elapsed ms.")
    }
  }

}
