package ir.example.chapter19;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static ir.example.util.PathUtility.PROJECT_INPUT_PATH;

import java.util.Scanner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC19Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC19Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.streaming.blockInterval", "5s")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(20_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);

        JavaDStream<String> dStream = javaStreamingContext.textFileStream(PROJECT_INPUT_PATH);
        //  JavaDStream<String> dStream = javaStreamingContext.binaryRecordsStream(PROJECT_INPUT_PATH, 2)
        //        .map(String::new);

        dStream.print();

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
