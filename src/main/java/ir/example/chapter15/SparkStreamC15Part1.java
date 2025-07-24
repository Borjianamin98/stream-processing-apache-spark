package ir.example.chapter15;

import static ir.example.util.HadoopUtility.createHadoopDirectory;
import static org.apache.spark.sql.functions.col;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamC15Part1 {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamC15Part1.class);

    public static void main(String[] args) {
        createHadoopDirectory();

        SparkSession sparkSession = SparkSession.builder()
                .appName("example")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> stream = sparkSession.readStream()
                .format("rate")
                .option("rowsPerSecond", "5")
                .load();

        Dataset<Row> evenElements = stream
                .select(col("timestamp"), col("value"))
                .where(col("value").mod(2).equalTo(0));

        StreamingQuery query;
        try {
            query = evenElements
                    .writeStream()
                    .queryName("sample")
                    .trigger(Trigger.Continuous("10 seconds"))
                    .outputMode("append")
                    .format("console")
                    .option("numRows", 10)
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
