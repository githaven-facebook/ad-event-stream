plugins {
    kotlin("jvm")
}

val flinkVersion: String by project
val flinkScalaVersion: String by project
val kafkaVersion: String by project
val jacksonVersion: String by project
val avroVersion: String by project
val slf4jVersion: String by project
val log4jVersion: String by project

dependencies {
    // Flink core
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime:$flinkVersion")

    // Flink Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

    // Flink file system / S3 (Parquet)
    implementation("org.apache.flink:flink-parquet:$flinkVersion")
    compileOnly("org.apache.flink:flink-s3-fs-hadoop:$flinkVersion")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Avro
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("org.apache.parquet:parquet-avro:1.13.1")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")

    // Flink test utilities
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
}

tasks.jar {
    archiveBaseName.set("stream-common")
}
