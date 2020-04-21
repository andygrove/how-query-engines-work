plugins {
    kotlin("plugin.serialization") version "1.3.61"
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.ballistacompute"
            artifactId = "physical-plan"
            version = "0.2.0"

            from(components["kotlin"])
        }
    }
}

dependencies {

    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")
}
