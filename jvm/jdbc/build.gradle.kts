plugins {
    java
}


dependencies {

    implementation("org.apache.arrow:flight-core:0.16.0")
    implementation("org.apache.arrow:flight-grpc:0.16.0")

    testImplementation("junit:junit:4.13")
}


tasks.test {
    //useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
