package ir.example.chapter9;

import static ir.example.kafka.KafkaServerAsProducer.TOPIC_NAME;
import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC9Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC9Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> stream = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:6726")
                .option("subscribe", TOPIC_NAME)
                .option("startingOffsets", "earliest")
                .load();

        /**
         * See {@link org.apache.spark.sql.kafka010.KafkaSourceProvider} for how implemented
         * See {@link org.apache.kafka.common.record.TimestampType} for different values of timestamp type
         * Kafka client config: message.timestamp.type
         *                      Define whether the timestamp in the message is 'create time' or 'log append time'.
         * This helps you determine whether you're working with producer-assigned timestamps (which reflect when
         * events actually occurred) or broker-assigned timestamps (which reflect when Kafka received the messages).
         */
        stream.printSchema();

        StreamingQuery query;
        try {
            query = stream
                    .writeStream()
                    .trigger(Trigger.ProcessingTime("20 seconds"))
                    .outputMode("append")
                    .format("console")
                    .option("numRows", 30)
                    .option("truncate", false)
                    .start();
        } catch (TimeoutException e) {
            throw new AssertionError("Timeout on query start", e);
        }

        Scanner scanner = new Scanner(System.in);
        while (!scanner.nextLine().equals("exit")) {
            log.debug("Ignore non-exit command");
        }

        try {
            query.stop();
        } catch (TimeoutException e) {
            throw new AssertionError("Failed to stop query", e);
        }
        sparkSession.stop();
    }
}
