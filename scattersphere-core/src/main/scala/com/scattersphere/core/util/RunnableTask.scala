package com.scattersphere.core.util

abstract class RunnableTask extends Runnable {

  def onException(exception: Throwable): Unit = {
    println(s"Exception $exception occurred")
    exception.printStackTrace()
  }

  def onCancel(): Unit = {
    println(s"Task canceled")
  }

  def onFinished(): Runnable = () => println(s"Job finished.")

}

object RunnableTask {

  def apply(runnable: Runnable): RunnableTask = new RunnableTask {
    override def run(): Unit = {
      runnable.run()
    }
  }

}