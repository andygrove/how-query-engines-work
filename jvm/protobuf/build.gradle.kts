import com.google.protobuf.gradle.*

description = "KQuery protocol buffer format"

plugins {
    java
    id("com.google.protobuf") version "0.8.18"
    id("idea")
}

protobuf {
    protoc {
        // Use protoc 3.21.12 - earliest stable version with Apple Silicon support
        artifact = "com.google.protobuf:protoc:3.21.12"
    }
}


sourceSets {
    main {
        proto {
            srcDir("../../proto")
        }
        java{
            srcDir("build/generated/proto/main/java")
        }
    }
}

dependencies {

    implementation(project(":datatypes"))
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))

    implementation("org.apache.arrow:arrow-memory:0.17.0")
    implementation("org.apache.arrow:arrow-vector:0.17.0")
    implementation("com.google.protobuf:protobuf-java:3.21.12")
}
