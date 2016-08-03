package io.infoworks

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

import org.apache.commons.cli._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{mutable => m}

/**
  * Merges incremental files with existing files. Performs following actions:
  *   1) Retrieves & validates command line arguments.
  *   2) Starts a thread for each combination of existing & incremental directory and puts this thread in a thread pool.
  */
object MergeDriver extends {

  def main(args: Array[String]): Unit = {

    val options = new Options()

    val existing = new Option("e", "existing", true, "comma separated list of existing directories")
    existing.setRequired(true)
    options.addOption(existing)

    val incremental = new Option("i", "incremental", true, "comma separated list of increment directories")
    incremental.setRequired(true)
    options.addOption(incremental)

    val output = new Option("o", "output", true, "comma separated list of output directories")
    output.setRequired(true)
    options.addOption(output)

    val poolSize = new Option("p", "pool size", true, "number of directories to process simulteneously. Default is ALL")
    options.addOption(poolSize)

    val key = new Option("k", "key column name", true, "Key Column Name")
    options.addOption(key)

    val parser = new GnuParser()
    val formatter = new HelpFormatter()

    var cmd: CommandLine = null

    try {
      cmd = parser.parse(options, args)
    } catch {
      case e: Exception =>
        println("Exception: " + e.getMessage)
        formatter.printHelp("Merge: ", options)
        System.exit(1)
        return
    }

    val existingPaths = cmd.getOptionValue("e").split(",").map(_.trim)
    val incrementalPaths = cmd.getOptionValue("i").split(",")
    if (existingPaths.length != incrementalPaths.length) {
      formatter.printHelp("No. of paths in 'existing' should be same as no. of paths in 'incremental'", options)
    }
    val outputPaths = cmd.getOptionValue("o").split(",").map(_.trim)
    if (existingPaths.length != outputPaths.length) {
      formatter.printHelp("No. of paths in 'existing' should be same as no. of paths in 'output'", options)
    }
    val threadPoolSize = cmd.getOptionValue("p", existingPaths.length.toString).toInt
    if (threadPoolSize < 0 || threadPoolSize > existingPaths.length) {
      formatter.printHelp("Pool size value is incorrect", options)
    }
    val keyColumn = cmd.getOptionValue("k", "")

    System.exit(merge(existingPaths, incrementalPaths, outputPaths, threadPoolSize, keyColumn))
  }

  def merge(existingPaths: Array[String], incrementalPaths: Array[String], outputPaths: Array[String],
            threadPoolSize: Int, keyColumn: String): Int = {

    val conf = new SparkConf().setAppName("Merge process...")
    val sc = new SparkContext(conf)

    val executor: ExecutorService = Executors.newFixedThreadPool(threadPoolSize)
    val calls: m.Map[Int, Future[Boolean]] = m.HashMap()

    for (idx <- existingPaths.indices) {
      println("Submitting job for path: %s".format(existingPaths(idx)))
      val callable: Callable[Boolean] = new Merger(sc, existingPaths(idx), incrementalPaths(idx), outputPaths(idx), keyColumn)

      // Submit the method to the thread pool.
      val future: Future[Boolean] = executor.submit(callable)

      // Put the 'future' object in a Map so we can check return value after the thread completes.
      calls.put(idx, future)
    }

    // If any call fails, return -1; else return 0
    var success: Int = 0
    calls.foreach { call => {
      val future: Future[Boolean] = call._2
      if (!future.get()) {
        success = -1
      }
    }
    }

    // Shutdown thread pool.
    executor.shutdown()
    success
  }
}
