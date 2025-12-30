description = "KQuery query optimizer"

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))

    implementation("org.apache.arrow:arrow-memory:18.3.0")
    implementation("org.apache.arrow:arrow-vector:18.3.0")

}
