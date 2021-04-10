description = "Ballista Spark Executor"

plugins {
    scala
    application
}

application {
    mainClassName = "io.andygrove.queryengine.spark.executor.SparkExecutor"
}

dependencies {
}
