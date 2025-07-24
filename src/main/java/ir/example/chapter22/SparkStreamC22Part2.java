package ir.example.chapter22;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.example.socket.WebLog;
import ir.example.util.PathUtility;
import java.io.IOException;
import java.util.Objects;
import java.util.Scanner;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamC22Part2 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC22Part2.class);

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

        Function3<String, Optional<Long>, State<Long>, Long> mappingFunction =
                (host, value, state) -> {
                    return value.orElse(0L);
                };
        // There are 2 constructor with same input but one of them is not serializable because it is for scala.
        // You should use correct one which is for java code. Also ensure correct `imports` from spark package
        // not from java package!
        StateSpec<String, Long, Long, Long> stateSpec = StateSpec
                .function(mappingFunction)
                .timeout(Duration.apply(20_000));

        JavaPairDStream<String, Long> hostBytes = webLogJavaDStream.mapToPair(
                record -> new Tuple2<>(record.getHost(), record.getBytes()));

        JavaMapWithStateDStream<String, Long, Long, Long> hostBytesWithState = hostBytes.mapWithState(stateSpec);
        JavaPairDStream<String, Long> stateSnapshots = hostBytesWithState.stateSnapshots();

        stateSnapshots.print();
        hostBytesWithState.print();

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
