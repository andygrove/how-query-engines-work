description = "Ballista Spark Benchmarks"

plugins {
    scala
    application
}

application {
    mainClassName = "io.andygrove.kquery.spark.benchmarks.Main"
}
dependencies {
//    implementation("org.apache.arrow:arrow-memory:0.17.0")
//    implementation("org.apache.arrow:arrow-vector:0.17.0")
}
