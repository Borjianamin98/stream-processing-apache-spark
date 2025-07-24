package ir.example.chapter13;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import ir.example.socket.WebLog;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC13Part2 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC13Part2.class);

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

        KeyValueGroupedDataset<String, WebLog> webLogGroupedByHost = webLogStream
                .groupByKey((MapFunction<WebLog, String>) WebLog::getHost, Encoders.STRING());

        FlatMapGroupsWithStateFunction<String, WebLog, BufferState, WebLogBytesAverage> mappingFunction =
                (key, values, state) -> {
                    int elementWindowSize = 5;
                    BufferState currentState = state.getOption().getOrElse(() -> new BufferState(elementWindowSize));

                    BufferState updatedState = new BufferState(currentState);
                    while (values.hasNext()) {
                        updatedState.add(values.next());
                    }
                    state.update(updatedState);

                    List<WebLog> data = updatedState.getData();
                    if (data.size() == elementWindowSize) {
                        WebLog start = data.get(0);
                        WebLog end = data.get(data.size() - 1);

                        double bytesSum = 0.0;
                        for (WebLog event : data) {
                            bytesSum += event.getBytes();
                        }
                        double bytesAvg = bytesSum / data.size();
                        return Collections.singleton(new WebLogBytesAverage(
                                key, start.getTimestamp(), end.getTimestamp(), bytesAvg)).iterator();
                    } else {
                        return Collections.emptyIterator();
                    }
                };

        Dataset<WebLogBytesAverage> webLogBytesAverageDataset = webLogGroupedByHost.flatMapGroupsWithState(
                mappingFunction,
                OutputMode.Update(),
                Encoders.bean(BufferState.class),
                Encoders.bean(WebLogBytesAverage.class),
                GroupStateTimeout.ProcessingTimeTimeout());

        StreamingQuery query;
        try {
            query = webLogBytesAverageDataset
                    .writeStream()
                    .format("console")
                    .outputMode("update")
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
