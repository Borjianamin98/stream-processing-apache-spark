package ir.example.chapter13;

import ir.example.socket.WebLog;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class BufferState implements Serializable {

    private int capacity;
    private List<WebLog> data;

    public BufferState() {
        // Used by spark streaming internally
    }

    public BufferState(int capacity) {
        this.capacity = capacity;
        this.data = new LinkedList<>();
    }

    public BufferState(BufferState clone) {
        this.capacity = clone.capacity;
        this.data = new LinkedList<>();
        if (clone.data != null) {
            clone.data.addAll(this.data);
        }
    }

    // Getter/setter required otherwise strange error happen!
    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public List<WebLog> getData() {
        return data;
    }

    public void setData(List<WebLog> data) {
        this.data = data;
    }

    public void add(WebLog item) {
        data.add(item);
        if (data.size() > capacity) {
            data.remove(0);
        }
    }
}