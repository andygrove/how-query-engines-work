plugins {
    kotlin("jvm") version "1.3.60"
}


dependencies {

    implementation(kotlin("stdlib-jdk8"))

    // Gradle plugin
    implementation(gradleApi())

    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))

    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    // Use the Kotlin test library.
    testImplementation("org.jetbrains.kotlin:kotlin-test")

    // Use the Kotlin JUnit integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.junit.jupiter:junit-jupiter:5.5.2")
    testImplementation("junit:junit:4.12")
    
    compile(project(":protobuf"))

    implementation("org.apache.arrow:flight-core:0.16.0")
    implementation("org.apache.arrow:flight-grpc:0.16.0")

}


tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
