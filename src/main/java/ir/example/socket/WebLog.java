package ir.example.socket;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class WebLog implements Serializable {

    private final String dateFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private transient final SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);

    private String host;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = dateFormatString)
    private Timestamp timestamp;
    private String request;
    @JsonProperty("http_reply")
    private Integer httpReply;
    private Long bytes;

    public WebLog() {
    }

    public WebLog(String host, Timestamp timestamp, String request, Integer httpReply, Long bytes) {
        this.host = host;
        this.timestamp = timestamp;
        this.request = request;
        this.httpReply = httpReply;
        this.bytes = bytes;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public Integer getHttpReply() {
        return httpReply;
    }

    public void setHttpReply(Integer httpReply) {
        this.httpReply = httpReply;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WebLog webLog = (WebLog) o;
        return Objects.equals(host, webLog.host) && Objects.equals(timestamp, webLog.timestamp)
                && Objects.equals(request, webLog.request) && Objects.equals(httpReply,
                webLog.httpReply) && Objects.equals(bytes, webLog.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, timestamp, request, httpReply, bytes);
    }

    @Override
    public String toString() {
        return "WebLog{" +
                "host='" + host + '\'' +
                ", timestamp=" + timestamp +
                ", request='" + request + '\'' +
                ", http_reply=" + httpReply +
                ", bytes=" + bytes +
                '}';
    }

    public String toJson() {
        List<String> fields = new ArrayList<>();
        if (host != null) {
            fields.add("\"host\":\"" + host + "\"");
        }
        if (timestamp != null) {
            fields.add("\"timestamp\":\"" + dateFormat.format(timestamp) + "\"");
        }
        if (request != null) {
            fields.add("\"request\":\"" + request + "\"");
        }
        if (httpReply != null) {
            fields.add("\"http_reply\":\"" + httpReply + "\"");
        }
        fields.add("\"bytes\":" + (bytes != null ? bytes : 0));
        return "{" + String.join(",", fields) + "}";
    }
}
