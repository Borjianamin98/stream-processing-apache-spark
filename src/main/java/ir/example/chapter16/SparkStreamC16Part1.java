package ir.example.chapter16;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Scanner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC16Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC16Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(20_000));

        // Use 'nc -lk 9876' to bring up server
        ReceiverInputDStream<String> dStream = streamingContext.socketTextStream(
                "localhost", 9876, StorageLevel.MEMORY_AND_DISK());

        DStream<Object> countStream = dStream.count();
        countStream.print();

        streamingContext.start();

        Scanner scanner = new Scanner(System.in);
        while (!scanner.nextLine().equals("exit")) {
            log.debug("Ignore non-exit command");
        }

        streamingContext.stop(false, true);
        sparkSession.stop();
    }
}
