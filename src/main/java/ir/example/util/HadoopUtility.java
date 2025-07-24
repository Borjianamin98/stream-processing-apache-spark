package ir.example.util;

import ir.example.chapter10.SparkStreamC10Part1;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtility {

    private static final Logger log = LoggerFactory.getLogger(HadoopUtility.class);

    private HadoopUtility() {
    }

    public static void createHadoopDirectory() {
        // Create a temporary hadoop bin directory to satisfy Spark's requirements
        try {
            String tempDirectory = Files.createTempDirectory("hadoop").toAbsolutePath().toString();

            Path hadoopBinDirectory = Paths.get(tempDirectory, "bin");
            Files.createDirectory(hadoopBinDirectory);

            URL resourceUrl = SparkStreamC10Part1.class.getClassLoader().getResource("hadoop-2.8.3-bin");
            Path resourcePath = Paths.get(Objects.requireNonNull(resourceUrl).toURI());
            try (Stream<Path> stream = Files.walk(resourcePath)) {
                stream.forEach(sourcePath -> {
                    try {
                        Path targetPath = hadoopBinDirectory.resolve(resourcePath.relativize(sourcePath));
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            System.setProperty("hadoop.home.dir", tempDirectory);
            System.load(resourcePath + "/hadoop.dll");
            log.info("Hadoop temp directory: {}", tempDirectory);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    FileUtils.deleteDirectory(Paths.get(tempDirectory).toFile());
                } catch (Exception e) {
                    throw new AssertionError("Unable to delete temp directory: " + tempDirectory, e);
                }
            }));
        } catch (IOException | URISyntaxException e) {
            throw new AssertionError("Unable to create hadoop directory", e);
        }
    }
}
