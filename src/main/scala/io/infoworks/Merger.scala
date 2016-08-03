package io.infoworks

import java.util.concurrent.Callable

import org.apache.spark.SparkContext
import org.apache.spark.sql.{functions => f}

/**
  * Merges incremental data into existing data by doing a 'full outer join' & selecting 'coalesce' values of each
  * column.
  */
class Merger(sc: SparkContext, existingPath: String, incrementalPath: String, outputPath: String, keyColumn: String)
  extends Callable[Boolean] {

  val random = scala.util.Random

  override def call(): Boolean = {

    try {
      val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

      // Create temporary table of existing data.
      val existing = sqlContext.read.format("orc").load(existingPath)
      val existingTable = "existing_%d".format(random.nextInt(1000))
      existing.registerTempTable(existingTable)

      // Create temporary table of incremental data.
      val incremental = sqlContext.read.format("orc").load(incrementalPath)
      val incrementalTable = "incremental_%d".format(random.nextInt(1000))
      incremental.registerTempTable(incrementalTable) // add random num

      // The following statements build a 'select' clause in the following format:
      //   SELECT COALESCE(incremental.empno, existing.empno) as empno,COALESCE(incremental.name, existing.name) as name,
      //     COALESCE(incremental.deptno, existing.deptno) as deptno FROM existing FULL OUTER JOIN incremental
      //     ON existing.empno = incremental.empno";

      var select: String = "SELECT "
      select = select + existing.columns.map(colName => "COALESCE(%s.%s, %s.%s) as %s".format(incrementalTable, colName,
        existingTable, colName, colName)).mkString(",")
      select = select + " FROM %s FULL OUTER JOIN %s ON %s.%s = %s.%s".format(existingTable, incrementalTable,
        existingTable, keyColumn, incrementalTable, keyColumn)

      println("******************")
      println(select)
      println("******************")

      // Run the Sql & save it in the ORC format
      // Do this in version 1.5 & above: sqlContext.sql(select).write.orc(outputPath)
      sqlContext.sql(select).write.format("orc").save(outputPath)

    } catch {
      case e: Exception =>
        println("Exception: " + e.getMessage)
        return false
    }
    true
  }
}
