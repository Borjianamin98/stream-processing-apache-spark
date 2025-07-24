package ir.example.chapter11;

import ir.example.socket.TCPWriter;
import ir.example.socket.WebLog;
import java.io.IOException;
import java.io.Serializable;
import org.apache.spark.sql.ForeachWriter;

public class TCPForeachWriter extends ForeachWriter<WebLog> implements Serializable {

    private transient TCPWriter writer;
    private long localPartition;
    private long localVersion;
    private final String host;
    private final int port;

    public TCPForeachWriter(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean open(long partitionId, long version) {
        try {
            this.writer = new TCPWriter(host, port);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.localPartition = partitionId;
        this.localVersion = version;
        System.out.printf("Writing partition [%d] and version [%d] %n", partitionId, version);
        return true; // we always accept to write
    }

    @Override
    public void process(WebLog value) {
        writer.println(String.format("%d, %d, %s", localPartition, localVersion, value.toJson()));
    }

    @Override
    public void close(Throwable errorOrNull) {
        if (errorOrNull == null) {
            System.out.printf("Closing partition [%d] and version[%d]%n", localPartition, localVersion);
            writer.close();
        } else {
            System.out.println("Query failed with: " + errorOrNull);
        }
    }
}
