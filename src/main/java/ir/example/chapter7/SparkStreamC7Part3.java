package ir.example.chapter7;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

import ir.example.socket.WebLog;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC7Part3 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC7Part3.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
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

        Pattern urlExtractor = Pattern.compile("^GET (.+) HTTP/\\d.\\d");
        List<String> allowedExtensions = Arrays.asList(".html", ".htm", "");
        Dataset<WebLog> urlWebLogStream = webLogStream.flatMap(
                (FlatMapFunction<WebLog, WebLog>) weblog -> {
                    String request = weblog.getRequest();
                    if (request == null) {
                        return Collections.emptyIterator();
                    }

                    Matcher matcher = urlExtractor.matcher(request);
                    if (!matcher.find()) {
                        return Collections.emptyIterator();
                    }

                    String url = matcher.group(1);
                    String ext = url.substring(Math.max(0, url.length() - 5))
                            .replaceAll("^[^.]*", "");
                    if (!allowedExtensions.contains(ext)) {
                        return Collections.emptyIterator();
                    }
                    WebLog modified = new WebLog(
                            weblog.getHost(),
                            weblog.getTimestamp(),
                            url,
                            weblog.getHttpReply(),
                            weblog.getBytes()
                    );
                    return Collections.singletonList(modified).iterator();
                }, Encoders.bean(WebLog.class));

        StreamingQuery query;
        try {
            query = urlWebLogStream
                    .writeStream()
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
