description = "KQuery query executor"

plugins {
    kotlin("plugin.serialization") version "1.9.22"
    application
}

application {
    mainClass.set("io.andygrove.kquery.executor.Executor")
}

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":sql"))
    implementation(project(":protobuf"))
    implementation(project(":execution"))

    implementation("org.apache.arrow:arrow-vector:18.3.0")

    implementation("org.apache.arrow:flight-core:18.3.0")
}
