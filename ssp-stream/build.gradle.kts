plugins {
    kotlin("jvm")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

val flinkVersion: String by project
val flinkScalaVersion: String by project
val kafkaVersion: String by project
val jacksonVersion: String by project
val slf4jVersion: String by project
val log4jVersion: String by project
val hadoopVersion: String by project

dependencies {
    implementation(project(":stream-common"))

    // Flink core (provided at runtime by Flink cluster)
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")
    compileOnly("org.apache.flink:flink-runtime:$flinkVersion")

    // Flink Kafka connector
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")

    // Flink file system connectors
    implementation("org.apache.flink:flink-parquet:$flinkVersion")
    implementation("org.apache.flink:flink-hadoop-compatibility:$flinkVersion")
    compileOnly("org.apache.flink:flink-s3-fs-hadoop:$flinkVersion")

    // Hadoop Parquet dependencies
    implementation("org.apache.hadoop:hadoop-common:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion") {
        exclude(group = "org.slf4j")
        exclude(group = "log4j")
    }
    implementation("org.apache.parquet:parquet-hadoop:1.13.1")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")

    // Test dependencies
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
}

tasks.shadowJar {
    archiveBaseName.set("ssp-stream")
    archiveClassifier.set("all")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "com.facebook.ads.stream.ssp.SSPStreamApp"
    }
    // Exclude Flink classes that are provided by the cluster
    dependencies {
        exclude(dependency("org.apache.flink:flink-streaming-java:$flinkVersion"))
        exclude(dependency("org.apache.flink:flink-clients:$flinkVersion"))
        exclude(dependency("org.apache.flink:flink-runtime:$flinkVersion"))
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
