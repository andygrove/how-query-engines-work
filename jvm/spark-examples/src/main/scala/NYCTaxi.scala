package org.ballistacompute.examples.spark

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object NYCTaxi {

  /*VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
  */

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
    StructField("total_amount", DataTypes.DoubleType),
    StructField("congestion_surcharge", DataTypes.DoubleType)
  ))


}
