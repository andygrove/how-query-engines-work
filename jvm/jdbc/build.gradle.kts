description = "KQuery JDBC Driver"

plugins {
    java
}

dependencies {

    implementation("org.apache.arrow:flight-core:18.3.0")

    testImplementation("junit:junit:4.13")
}

tasks.test {
    //useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
