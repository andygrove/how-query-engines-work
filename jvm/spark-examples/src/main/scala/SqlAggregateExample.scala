package org.ballistacompute.examples.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object SqlAggregateExample {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]") // use a single thread for fair comparison to current kotlin-query functionality
      .getOrCreate()

    val tripdata = spark.read.format("csv")
      .option("header", "true")
      .schema(NYCTaxi.schema)
      .load("/home/andy/data/yellow_tripdata_2019-01.csv")

    tripdata.createOrReplaceTempView("tripdata")

    val start = System.currentTimeMillis()

    val df = spark.sql(
      """SELECT passenger_count, MAX(fare_amount)
        |FROM tripdata
        |GROUP BY passenger_count""".stripMargin)

    df.explain()

    df.foreach(row => println(row))

    val duration = System.currentTimeMillis() - start

    println(s"Query took $duration ms")


  }

}