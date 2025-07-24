package ir.example.chapter27;

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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.sketch.CountMinSketch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamC27Part2 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC27Part2.class);

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

        JavaPairDStream<String, Long> hostsCount = webLogJavaDStream
                .mapToPair(record -> new Tuple2<>(record.getHost(), record.getBytes()))
                .reduceByKey(Long::sum);

        Function3<String, Optional<Long>, State<CountMinSketch>, Void> mappingFunction =
                (host, value, state) -> {
                    if (state.isTimingOut()) {
                        return null;
                    }

                    CountMinSketch stateValue;
                    if (state.exists()) {
                        stateValue = state.get();
                    } else {
                        stateValue = CountMinSketch.create(0.001, 0.99, 1);
                    }

                    Long newSeenValue = value.orElse(0L);
                    long roundedValue;
                    if (newSeenValue >= 0 & newSeenValue < 5000) {
                        roundedValue = 4000;
                    } else if (newSeenValue >= 5000 & newSeenValue < 10000) {
                        roundedValue = 8000;
                    } else if (newSeenValue >= 10000 & newSeenValue < 20000) {
                        roundedValue = 15000;
                    } else {
                        roundedValue = 30000;
                    }
                    // It is sample code and estimated count is not necessary for 4 different values!!
                    stateValue.add(roundedValue);
                    state.update(stateValue);

                    return null;
                };

        StateSpec<String, Long, CountMinSketch, Void> stateSpec = StateSpec
                .function(mappingFunction)
                .timeout(Duration.apply(20_000));

        JavaDStream<Tuple2<String, Long>> hostEstimatedOfRecordSize8000 = hostsCount.mapWithState(stateSpec)
                .stateSnapshots()
                .map(tuple -> new Tuple2<>(tuple._1, tuple._2.estimateCount(8000L)));

        hostEstimatedOfRecordSize8000.print();

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
