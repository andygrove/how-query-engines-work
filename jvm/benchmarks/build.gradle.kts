plugins {
    kotlin("plugin.serialization") version "1.9.22"
    scala
    application
}

application {
    mainClass.set("io.andygrove.kquery.benchmarks.Benchmarks")
}

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":optimizer"))
    implementation(project(":physical-plan"))
    implementation(project(":execution"))
    implementation(project(":sql"))

    implementation("org.apache.arrow:arrow-vector:18.3.0")
}
