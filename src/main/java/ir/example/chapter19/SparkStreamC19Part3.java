package ir.example.chapter19;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import java.util.Collections;
import java.util.Scanner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ConstantInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

public class SparkStreamC19Part3 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC19Part3.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .config("spark.streaming.blockInterval", "5s")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(20_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);

        String data = "Chimay, Ciney, Corsendonck, Duivel, Chimay, Corsendonck";
        JavaRDD<String> rdd = javaStreamingContext.sparkContext().parallelize(Collections.singletonList(data));
        ConstantInputDStream<String> constantInputDStream = new ConstantInputDStream<>(streamingContext,
                rdd.rdd(), ClassTag.apply(String.class));

        constantInputDStream.print();

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
