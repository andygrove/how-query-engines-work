plugins {
    kotlin("plugin.serialization") version "1.3.61"
    scala
}


dependencies {
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":optimizer"))
    implementation(project(":physical-plan"))
    implementation(project(":sql"))
    implementation(project(":execution"))

    implementation("org.apache.arrow:arrow-vector:0.16.0")
}
