package ir.example.chapter12;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.window;

import ir.example.socket.WebLog;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC12Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC12Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.sql.shuffle.partitions", "10")
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

        Dataset<Row> perHalfMinuteAvg = webLogStream
                .withWatermark("timestamp", "10 seconds")
                .groupBy(window(col("timestamp"), "10 seconds"))
                .agg(avg(col("bytes")), count(col("bytes")));

        StreamingQuery query;
        try {
            query = perHalfMinuteAvg
                    .writeStream()
                    .format("console")
                    .outputMode("append")
                    .option("truncate", false)
                    .trigger(Trigger.ProcessingTime("20 seconds"))
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
