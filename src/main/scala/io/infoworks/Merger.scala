package io.infoworks

import java.util.concurrent.Callable

import org.apache.spark.SparkContext

/**
  * Merges incremental data into existing data by doing a 'full outer join' & selecting new value, if it's available.
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
      incremental.registerTempTable(incrementalTable)

      // The following statements build a 'select' clause in the following format:
      // SELECT IF(ISNULL(incremental_446.empno), existing_973.empno, incremental_446.empno) as empno,
      //   IF(ISNULL(incremental_446.empno), existing_973.name, incremental_446.name) as name,
      //   IF(ISNULL(incremental_446.empno), existing_973.deptno, incremental_446.deptno) as deptno
      // FROM existing_973 FULL OUTER JOIN incremental_446 ON existing_973.empno = incremental_446.empno

      var select: String = "SELECT "
      select = select + existing.columns.map(colName => "IF(ISNULL(%s.%s), %s.%s, %s.%s) as %s".format(incrementalTable,
        keyColumn, existingTable, colName, incrementalTable, colName, colName)).mkString(",")
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
