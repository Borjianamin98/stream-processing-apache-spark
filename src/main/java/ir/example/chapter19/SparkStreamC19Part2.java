package ir.example.chapter19;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class SparkStreamC19Part2 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC19Part2.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.streaming.blockInterval", "5s")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(20_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);

        Queue<JavaRDD<String>> queue = new LinkedBlockingQueue<>(100);
        List<String> data = Arrays.asList(
                "Chimay, Ciney, Corsendonck, Duivel, Chimay, Corsendonck ",
                "Leffe, Ciney, Leffe, Ciney, Grimbergen, Leffe, La Chouffe, Leffe",
                "Leffe, Hapkin, Corsendonck, Leffe, Hapkin, La Chouffe, Leffe"
        );
        // Create a list of RDDs, each containing a String of words
        List<JavaRDD<String>> rdds = new ArrayList<>();
        for (String sentence : data) {
            JavaRDD<String> rdd = javaStreamingContext.sparkContext().parallelize(Collections.singletonList(sentence));
            rdds.add(rdd);
        }
        queue.addAll(rdds);

        JavaPairDStream<String, Long> wordCountStream = javaStreamingContext.queueStream(queue, true)
                .flatMap(sentence -> Arrays.asList(sentence.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word.trim(), 1L))
                .reduceByKey(Long::sum);

        wordCountStream.print();

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
