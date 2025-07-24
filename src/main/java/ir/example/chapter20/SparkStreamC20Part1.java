package ir.example.chapter20;

import static ir.example.util.HadoopUtility.createHadoopDirectory;

import ir.example.socket.TCPWriter;
import java.util.Scanner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC20Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC20Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[4]")
                .getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), new Duration(5_000));
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);

        // Use 'nc -lk 9876' to bring up server
        JavaReceiverInputDStream<String> dStream = javaStreamingContext.socketTextStream(
                "localhost", 9876, StorageLevel.MEMORY_AND_DISK_SER_2());

        // Use 'nc -lk 9999' to bring up server
        dStream.foreachRDD(rdd -> {
            JavaRDD<String> filteredRdd = rdd.filter(element -> element.contains("host"));
            JavaRDD<String> recordRdd = filteredRdd.map(element -> "record is: " + element);
            recordRdd.foreachPartition(stringIterator -> {
                TCPWriter tcpWriter = new TCPWriter("localhost", 9999);
                stringIterator.forEachRemaining(tcpWriter::println);
            });
        });

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
