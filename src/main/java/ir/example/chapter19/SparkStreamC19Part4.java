package ir.example.chapter19;

import static ir.example.kafka.KafkaServerAsProducer.TOPIC_NAME;
import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamC19Part4 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC19Part4.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(10_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);

        List<String> topics = Collections.singletonList(TOPIC_NAME);
        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:" + 10475);
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", "c19part4");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", "true");
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // JavaDStream<String> kafkaRecordStream = directStream.map(record ->
        //        "key: " + record.key() + " value: " + record.value());
        JavaPairDStream<String, Long> kafkaRecordStream = directStream
                .mapToPair(record -> new Tuple2<>(record.key(), 1L))
                .reduceByKey(Long::sum);
        kafkaRecordStream.print();

        streamingContext.start();

        Scanner scanner = new Scanner(System.in);
        while (!scanner.nextLine().equals("exit")) {
            log.debug("Ignore non-exit command");
        }

        javaStreamingContext.stop(false, true);
        javaStreamingContext.close();
        sparkSession.stop();
    }

}
