package org.ballistacompute.examples.spark

import org.apache.spark.sql.SparkSession

object ConvertToParquet {

  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val tripdata = spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(NYCTaxi.schema)
      .load("/home/andy/data/nyctaxi/csv/2019/yellow_tripdata_2019-12.csv")

    tripdata
      .coalesce(1)
      .write
      .parquet("/home/andy/data/nyctaxi/parquet/2019/yellow_tripdata_2019-12.parquet")

  }
}
