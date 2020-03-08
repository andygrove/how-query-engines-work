package org.ballistacompute.examples.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object NYTaxiScalaExamples {

  def main(arg: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]") // use a single thread for fair comparison to current kotlin-query functionality
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("VendorID", DataTypes.IntegerType),
      StructField("tpep_pickup_datetime", DataTypes.TimestampType),
      StructField("tpep_dropoff_datetime", DataTypes.TimestampType),
      StructField("passenger_count", DataTypes.IntegerType),
      StructField("trip_distance", DataTypes.DoubleType),
      StructField("RatecodeID", DataTypes.IntegerType),
      StructField("store_and_fwd_flag", DataTypes.StringType),
      StructField("PULocationID", DataTypes.IntegerType),
      StructField("DOLocationID", DataTypes.IntegerType),
      StructField("payment_type", DataTypes.IntegerType),
      StructField("fare_amount", DataTypes.DoubleType),
      StructField("extra", DataTypes.DoubleType),
      StructField("mta_tax", DataTypes.DoubleType),
      StructField("tip_amount", DataTypes.DoubleType),
      StructField("tolls_amount", DataTypes.DoubleType),
      StructField("improvement_surcharge", DataTypes.DoubleType),
      StructField("total_amount", DataTypes.DoubleType)
    ))

    val tripdata = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
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