description = "KQuery Distributed Execution"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":optimizer"))
    implementation(project(":execution"))
    implementation(project(":sql"))
    implementation(project(":protobuf"))

    implementation("org.apache.arrow:arrow-memory:18.3.0")
    implementation("org.apache.arrow:arrow-vector:18.3.0")
    implementation("org.apache.arrow:flight-core:18.3.0")
}
