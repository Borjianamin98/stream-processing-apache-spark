package ir.example.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaServerAsConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServerAsConsumer.class);

    public static final String TOPIC_NAME = "topic-produce";

    public static void main(String[] args) throws IOException, InterruptedException {
        @SuppressWarnings("all")
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.3"))
                .withExposedPorts(9093);
        kafka.start();

        String bootstrapServers = kafka.getBootstrapServers();
        log.info("Kafka Server is running!");
        log.info("Internal bootstrap servers: {}", bootstrapServers);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "test");
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props)) {
            kafkaConsumer.subscribe(Collections.singleton(TOPIC_NAME));
            log.info("Going to consume records ...");
            log.info("Retrieve records from {} topic {} ...", bootstrapServers, TOPIC_NAME);

            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received record with key = {} and value = {}", record.key(), record.value());
                }
            }
        }
    }

}
