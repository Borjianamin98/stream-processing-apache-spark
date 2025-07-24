package ir.example.chapter11;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static ir.example.util.PathUtility.PROJECT_CHECKPOINT_PATH;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import ir.example.kafka.KafkaServerAsConsumer;
import ir.example.socket.WebLog;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC11Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC11Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
                .getOrCreate();

        // Use 'nc -lk 9876' to bring up server
        Dataset<Row> stream = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9876)
                .load();

        StructType webLogSchema = Encoders.bean(WebLog.class).schema();
        Dataset<Row> jsonStream = stream.select(from_json(col("value"), webLogSchema).as("record"));
        Dataset<WebLog> webLogStream = jsonStream.select("record.*").as(Encoders.bean(WebLog.class));

        Dataset<Row> kafkaFormattedStream = webLogStream.select(
                col("host").as("key"),
                functions.to_json(
                        functions.struct(
                                col("host"),
                                col("timestamp"),
                                col("request")
                        )
                ).as("value")
        );

        StreamingQuery query;
        try {
            query = kafkaFormattedStream
                    .writeStream()
                    .queryName("kafkaWriter")
                    .outputMode("append")
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:" + 10519)
                    .option("topic", KafkaServerAsConsumer.TOPIC_NAME)
                    .option("checkpointLocation", PROJECT_CHECKPOINT_PATH)
                    .option("failOnDataLoss", "false") // use this option when testing
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
