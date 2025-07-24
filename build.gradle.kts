plugins {
    id("java")
    id("application")
}

version = "0.1.0"
group = "ir.example"

repositories {
    mavenCentral()
}

dependencies {
    val sparkVersion = "3.2.3"
    implementation("org.apache.spark:spark-core_2.13:$sparkVersion")
    implementation("org.apache.spark:spark-streaming_2.13:$sparkVersion")
    implementation("org.apache.spark:spark-sql_2.13:$sparkVersion")
    implementation("org.apache.spark:spark-sql-kafka-0-10_2.13:$sparkVersion")
    implementation("org.apache.spark:spark-streaming-kafka-0-10_2.13:$sparkVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.0")

    implementation("commons-io:commons-io:2.19.0")

    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.slf4j:slf4j-log4j12:1.5.2")

    implementation("org.testcontainers:testcontainers:1.19.3")
    implementation("org.testcontainers:kafka:1.19.3")
    implementation("org.apache.kafka:kafka-clients:3.7.2")
}

tasks.withType<JavaExec> {
    jvmArgs = listOf(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio=ALL-UNNAMED",
        "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
}

tasks.test {
    useJUnitPlatform()
}