import java.time.Instant

plugins {
    scala
    kotlin("jvm") version "1.3.50" apply false
    id("com.diffplug.gradle.spotless") version "4.4.0"
}

group = "io.andygrove.kquery.spark"
version = "0.4.0-SNAPSHOT"
description = "KQuery Spark Support"

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
    }
}

subprojects {

    apply {
        plugin("org.jetbrains.kotlin.jvm")
        plugin("com.diffplug.gradle.spotless")
    }

    configurations.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "com.google.guava" && requested.name == "guava") {
                useVersion("32.1.3-jre")
                because("Force consistent Guava version to resolve variant conflicts between Arrow Flight and Spark/Hadoop")
            }
        }
    }

    spotless {
        scala {
            scalafmt()
        }
    }

    dependencies {

        implementation(kotlin("stdlib-jdk8"))
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.jetbrains.kotlin:kotlin-reflect")

        implementation("org.rogach:scallop_2.12:3.2.0")

        // note that this project depends on the kotlin artifacts being published to a local maven repository
        // see ../jvm/README.md for instructions on publishing those artifacts
        implementation("io.andygrove.kquery:datatypes:0.4.0-SNAPSHOT")
        implementation("io.andygrove.kquery:datasource:0.4.0-SNAPSHOT")
        implementation("io.andygrove.kquery:logical-plan:0.4.0-SNAPSHOT")
        implementation("io.andygrove.kquery:protobuf:0.4.0-SNAPSHOT")
        implementation("io.andygrove.kquery:executor:0.4.0-SNAPSHOT")

        implementation("org.apache.arrow:flight-core:18.3.0")

        implementation("org.apache.spark:spark-core_2.12:3.0.0")
        implementation("org.apache.spark:spark-sql_2.12:3.0.0")

        testImplementation("junit:junit:4.12")

    }

    tasks.jar {
        manifest {
            attributes(
                "Implementation-Title" to "${rootProject.name}-${archiveBaseName.get()}",
                "Implementation-Version" to rootProject.version,
                "Build-Timestamp" to Instant.now()
            )
        }
    }
}


