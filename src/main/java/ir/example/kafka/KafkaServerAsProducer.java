package ir.example.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaServerAsProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaServerAsProducer.class);

    public static final String TOPIC_NAME = "topic";

    public static void main(String[] args) throws IOException, InterruptedException {
        @SuppressWarnings("all")
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.3"))
                .withExposedPorts(9093);
        kafka.start();

        String bootstrapServers = kafka.getBootstrapServers();
        log.info("Kafka Server is running!");
        log.info("Internal bootstrap servers: {}", bootstrapServers);

        int partitionCount = 10;
        createTopic(bootstrapServers, TOPIC_NAME, partitionCount);

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
            log.info("Going to produce records ...");
            log.info("Retrieve records from {} topic {} ...", bootstrapServers, TOPIC_NAME);
            Random random = new Random();
            while (true) {
                int counter = 0;
                for (int i = 0; i < 2 * partitionCount; i++) {
                    kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, String.valueOf(counter),
                            counter + "," + random.nextInt(50)));
                    counter = (counter + 1) % partitionCount;
                }
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    private static void createTopic(String bootstrapServers, String topicName, int numPartitions)
            throws InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("Created topic: " + topicName);
            }
        } catch (ExecutionException e) {
            throw new AssertionError("Unable to create topic", e);
        }
    }

}
