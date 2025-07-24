package ir.example.chapter9;

import static ir.example.kafka.KafkaServerAsProducer.TOPIC_NAME;
import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Try;

public class SparkStreamC9Part2 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC9Part2.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> rawData = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:" + 7379)
                .option("subscribe", TOPIC_NAME)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<String> rawDataString = rawData.select("value").as(Encoders.STRING());
        Dataset<SensorData> iotData = rawDataString
                .flatMap((FlatMapFunction<String, SensorData>) record -> {
                    String[] fields = record.split(",");
                    Try<SensorData> sensorDataTry = Try.apply(() ->
                            new SensorData(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]))
                    );
                    if (sensorDataTry.isSuccess()) {
                        return java.util.Collections.singletonList(sensorDataTry.get()).iterator();
                    } else {
                        return java.util.Collections.emptyIterator();
                    }
                }, Encoders.bean(SensorData.class));

        rawData.printSchema();

        StreamingQuery query;
        try {
            query = iotData
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
