package ir.example.chapter18;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static ir.example.util.PathUtility.PROJECT_CHECKPOINT_PATH;

import java.util.Scanner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC18Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC18Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.streaming.blockInterval", "5s")
                .config("spark.streaming.receiver.writeAheadLog.enable", "true")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(20_000));
        streamingContext.checkpoint(PROJECT_CHECKPOINT_PATH);

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
