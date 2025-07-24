
package ir.example.socket;

import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketHandler {

    private static final Logger log = LoggerFactory.getLogger(SocketHandler.class);
    private static final int RECORD_DELAY_MILLIS = 500;

    private volatile boolean active = false;

    private final int port;
    private final List<WebLog> data;
    private final Timestamp minDataTimestamp;

    public SocketHandler(int port, List<WebLog> data) {
        this.port = port;
        this.data = data;

        this.minDataTimestamp = data.stream().map(WebLog::getTimestamp).min((o1, o2) -> {
            if (o1.before(o2)) {
                return -1;
            } else if (o1.after(o2)) {
                return 1;
            } else {
                return 0;
            }
        }).orElseThrow();
    }

    public void start() {
        active = true;
        new Thread(this::acceptConnections).start();
    }

    public void acceptConnections() {
        while (active) {
            try (ServerSocket server = new ServerSocket(port)) {
                Socket socket = server.accept();
                serve(socket);
            } catch (IOException e) {
                if (active) {
                    throw new AssertionError("Unexpected error", e);
                }
            }
        }
    }

    public void serve(Socket socket) {
        long now = System.currentTimeMillis();
        long offset = now - minDataTimestamp.getTime();

        List<String> jsonData = data.stream().map(webLog -> {
            Timestamp original = webLog.getTimestamp();
            Timestamp newTimestamp = new Timestamp(original.getTime() + offset);
            return new WebLog(
                    webLog.getHost(),
                    newTimestamp,
                    webLog.getRequest(),
                    webLog.getHttpReply(),
                    webLog.getBytes()
            );
        }).map(WebLog::toJson).toList();

        Iterator<String> jsonDataIterator = jsonData.iterator();

        log.info("Sent another round of logs ...");
        new Thread(() -> {
            try (PrintStream out = new PrintStream(socket.getOutputStream())) {
                out.println("HTTP/1.1 200 OK");
                out.println();
                while (jsonDataIterator.hasNext() && active) {
                    String data = jsonDataIterator.next();
                    out.println(data);
                    out.flush();
                    TimeUnit.MILLISECONDS.sleep(RECORD_DELAY_MILLIS);
                }
            } catch (IOException | InterruptedException e) {
                if (active) {
                    throw new AssertionError("Unexpected error", e);
                }
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new AssertionError("Unexpected error", e);
                }
            }
        }).start();
    }

}
