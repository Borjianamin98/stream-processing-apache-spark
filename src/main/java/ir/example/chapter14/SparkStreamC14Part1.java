package ir.example.chapter14;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC14Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC14Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
                .config("spark.sql.streaming.metricsEnabled", true)
                .getOrCreate();

        sparkSession.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                StreamingQueryProgress progress = event.progress();

                // ignore zero-valued events
                if (progress.numInputRows() > 0) {
                    String time = progress.timestamp();
                    double inputRowsPerSecond = progress.inputRowsPerSecond();
                    double processedRowsPerSecond = progress.processedRowsPerSecond();

                    System.out.println("---------------------- LISTENER REPORT ----------------------");
                    System.out.println("Progress Time: " + time);
                    System.out.println("Input Rows Per Second: " + inputRowsPerSecond);
                    System.out.println("Processed Rows Per Second: " + processedRowsPerSecond);
                    System.out.println("------------------- END OF LISTENER REPORT ------------------");
                }
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
            }

            @Override
            public void onQueryStarted(QueryStartedEvent event) {
            }
        });

        // Use 'nc -lk 9876' to bring up server
        Dataset<Row> stream = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9876)
                .load();

        StreamingQuery query;
        try {
            query = stream
                    .writeStream()
                    .queryName("sample")
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .outputMode("append")
                    .format("console")
                    .option("numRows", 5)
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
