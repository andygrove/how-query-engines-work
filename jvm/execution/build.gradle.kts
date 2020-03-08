plugins {
    kotlin("plugin.serialization") version "1.3.61"
}


dependencies {
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))
    implementation(project(":query-planner"))
    implementation(project(":sql"))

    implementation("org.apache.arrow:arrow-vector:0.16.0")
}
