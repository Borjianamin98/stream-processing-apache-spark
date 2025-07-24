package ir.example.socket;

import static ir.example.util.PathUtility.PROJECT_RESOURCE_PATH;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpServer {

    private static final Logger log = LoggerFactory.getLogger(TcpServer.class);

    public static void main(String[] args) throws IOException {
        int serverPort = 9876;
        String rootDir = PROJECT_RESOURCE_PATH + "\\nasa_dataset_july_1995";

        List<WebLog> webLogs = processDirectory(rootDir, ".json");
        log.info("Files loaded completely");

        SocketHandler server = new SocketHandler(serverPort, webLogs);
        server.start();
    }

    private static List<WebLog> processFile(Path filePath) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        try (Stream<String> lines = Files.lines(filePath)) {
            return lines.filter(line -> !line.trim().isEmpty())
                    .map(line -> {
                        try {
                            return mapper.readValue(line, WebLog.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to parse JSON: path=" + filePath + ", line=" + line, e);
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

    public static List<WebLog> processDirectory(String rootDir, String fileExtension) throws IOException {
        List<WebLog> allWebLogs = new ArrayList<>();
        List<Path> files = findFiles(rootDir, fileExtension);

        for (Path file : files) {
            log.info("Processing file: {}", file);
            List<WebLog> webLogs = processFile(file);
            allWebLogs.addAll(webLogs);
        }

        return allWebLogs;
    }

    public static List<Path> findFiles(String rootDir, String fileExtension) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(rootDir))) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(fileExtension))
                    .collect(Collectors.toList());
        }
    }
}
