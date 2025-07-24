package ir.example.chapter22;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.example.socket.WebLog;
import ir.example.util.PathUtility;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamC22Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC22Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.streaming.blockInterval", "1s")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(5_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
        javaStreamingContext.checkpoint(PathUtility.PROJECT_CHECKPOINT_PATH);

        // Use 'nc -lk 9876' to bring up server
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream(
                "localhost", 9876, StorageLevel.MEMORY_AND_DISK_SER_2());

        ObjectMapper mapper = new ObjectMapper();
        JavaDStream<WebLog> webLogJavaDStream = dStream.map(record -> {
            try {
                return mapper.readValue(record, WebLog.class);
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull);

        JavaPairDStream<String, Long> hostBytes = webLogJavaDStream
                .mapToPair(record -> new Tuple2<>(record.getHost(), record.getBytes()))
                .updateStateByKey((Function2<List<Long>, Optional<Long>, Optional<Long>>) (values, state) -> {
                    long newState = state.orElse(0L) + values.size();
                    if (newState > 0) {
                        return Optional.of(newState);
                    } else {
                        return Optional.absent();
                    }
                })
                .reduceByKey(Long::sum);

        hostBytes.print();

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
