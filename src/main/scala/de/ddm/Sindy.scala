package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    inputs.map(input => readData(input, spark))
      .map(table => {
        val attributes = table.columns
        table.flatMap(row => attributes.indices.map(x => (attributes(x), row.getString(x))))
      }).reduce((pair1, pair2) => pair1 union pair2)
      .groupByKey(t => t._2)
      .mapGroups((_, iterator) => iterator.map(x => x._1).toSet)
      .flatMap(s => s.map(elem => (elem, s - elem)))
      .groupByKey(row => row._1)
      .mapGroups((key, iter) => (key, iter.map(x => x._2).reduce(_ intersect _)))
      .collect()
      .filter(_._2.nonEmpty)
      .map(row => (row._1, row._2.toList.sorted))
      .sortBy(_._1)
      .foreach(row => println(row._1 + " < " + row._2.mkString(", ")))

    // TODO
  }
}
