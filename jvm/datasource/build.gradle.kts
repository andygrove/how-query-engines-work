description = "KQuery data sources"

dependencies {

    implementation(project(":datatypes"))

    implementation("org.apache.arrow:arrow-memory:18.3.0")
    implementation("org.apache.arrow:arrow-vector:18.3.0")

    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")
    implementation("org.apache.parquet:parquet-arrow:1.14.4")
    implementation("org.apache.parquet:parquet-common:1.14.4")
    implementation("org.apache.parquet:parquet-column:1.14.4")
    implementation("org.apache.parquet:parquet-hadoop:1.14.4")

    implementation("com.univocity:univocity-parsers:2.8.4")
}
