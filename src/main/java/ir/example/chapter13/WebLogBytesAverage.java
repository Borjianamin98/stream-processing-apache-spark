package ir.example.chapter13;

import java.sql.Timestamp;
import java.util.Objects;

public class WebLogBytesAverage {

    private String host;
    private Timestamp startTime;
    private Timestamp endTime;
    private Double bytesAvg;

    public WebLogBytesAverage() {
    }

    public WebLogBytesAverage(String host, Timestamp startTime, Timestamp endTime, Double bytesAvg) {
        this.host = host;
        this.startTime = startTime;
        this.endTime = endTime;
        this.bytesAvg = bytesAvg;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Timestamp getStartTime() {
        return startTime;
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public Double getBytesAvg() {
        return bytesAvg;
    }

    public void setBytesAvg(Double bytesAvg) {
        this.bytesAvg = bytesAvg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WebLogBytesAverage that = (WebLogBytesAverage) o;
        return Objects.equals(host, that.host) && Objects.equals(startTime, that.startTime)
                && Objects.equals(endTime, that.endTime) && Objects.equals(bytesAvg, that.bytesAvg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, startTime, endTime, bytesAvg);
    }

    @Override
    public String toString() {
        return "WebLogBytesAverage{" +
                "host='" + host + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", bytesAvg=" + bytesAvg +
                '}';
    }
}
