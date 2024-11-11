group = "org.ivcode"
version = "0.1.0-SNAPSHOT"

plugins {
    kotlin("jvm") version "1.9.22"
    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin
    api(kotlin("stdlib"))
    api(kotlin("reflect"))

    // Sql2o
    api("org.sql2o:sql2o:1.6.0")

    // Apache Commons
    api("commons-dbcp:commons-dbcp:1.4")

    // Drivers
    implementation("mysql:mysql-connector-java:8.0.26")
    implementation("org.postgresql:postgresql:42.7.4")

    // SLF4J
    api("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}