package ir.example.chapter22;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.example.socket.WebLog;
import ir.example.util.PathUtility;
import java.io.IOException;
import java.io.Serializable;
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

public class SparkStreamC22Part3 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC22Part3.class);

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

        Function3<String, Optional<Long>, State<AverageHolder>, Tuple2<String, Long>> mappingFunction =
                (host, value, state) -> {
                    if (state.isTimingOut()) {
                        return new Tuple2<>(host, 0L);
                    }

                    AverageHolder stateValue = new AverageHolder(0, 0.0);
                    if (state.exists()) {
                        stateValue = state.get();
                    }

                    // Keep average of bytes until now (based on timeout of state) per host as state.
                    // State snapshot give us this.
                    Long newSeenValue = value.orElse(0L);
                    double newAverage =
                            ((stateValue.getCountOfElements() * stateValue.getAverage()) + newSeenValue)
                                    /
                                    (stateValue.getCountOfElements() + 1);
                    state.update(new AverageHolder(stateValue.getCountOfElements() + 1, newAverage));

                    // Return maximum of bytes for each host if greater than double of average
                    // as output of `mapWithState`. `mapWithState` returns only one element for each key.
                    // So internally, this function called for all values of a batch (for example for all 100
                    // values of specific key) and generate one output value.
                    // output value do not contain key anymore and if you want value, you should provide it in output
                    // (like here that we used tuple of host and value)
                    return new Tuple2<>(host, newSeenValue > newAverage * 2 ? newSeenValue : 0);
                };
        // There are 2 constructor with same input but one of them is not serializable because it is for scala.
        // You should use correct one which is for java code. Also ensure correct `imports` from spark package
        // not from java package!
        StateSpec<String, Long, AverageHolder, Tuple2<String, Long>> stateSpec = StateSpec
                .function(mappingFunction)
                .timeout(Duration.apply(20_000));

        JavaPairDStream<String, Long> hostBytes = webLogJavaDStream.mapToPair(
                record -> new Tuple2<>(record.getHost(), record.getBytes()));

        JavaMapWithStateDStream<String, Long, AverageHolder, Tuple2<String, Long>> hostBytesWithState =
                hostBytes.mapWithState(stateSpec);

        JavaDStream<Tuple2<String, Long>> hostsWithBytesAnomaly = hostBytesWithState.filter(value -> value._2 > 0);
        JavaPairDStream<String, AverageHolder> stateSnapshots = hostBytesWithState.stateSnapshots();

        stateSnapshots.print(100);
        hostsWithBytesAnomaly.print(100);

        streamingContext.start();

        Scanner scanner = new Scanner(System.in);
        while (!scanner.nextLine().equals("exit")) {
            log.debug("Ignore non-exit command");
        }

        javaStreamingContext.stop(false, true);
        javaStreamingContext.close();
        sparkSession.stop();
    }

    public static class AverageHolder implements Serializable {

        private int countOfElements;
        private Double average;

        public AverageHolder() {
        }

        public AverageHolder(int countOfElements, Double average) {
            this.countOfElements = countOfElements;
            this.average = average;
        }

        public int getCountOfElements() {
            return countOfElements;
        }

        public void setCountOfElements(int countOfElements) {
            this.countOfElements = countOfElements;
        }

        public Double getAverage() {
            return average;
        }

        public void setAverage(Double average) {
            this.average = average;
        }

        @Override
        public String toString() {
            return "AverageHolder{" +
                    "countOfElements=" + countOfElements +
                    ", average=" + average +
                    '}';
        }
    }
}
