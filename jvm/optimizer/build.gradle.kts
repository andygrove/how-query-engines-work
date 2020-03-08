
dependencies {
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))

    implementation("org.apache.arrow:arrow-memory:0.16.0")
    implementation("org.apache.arrow:arrow-vector:0.16.0")

}
