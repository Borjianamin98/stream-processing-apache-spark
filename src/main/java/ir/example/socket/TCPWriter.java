package ir.example.socket;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.Socket;

public class TCPWriter implements Serializable {

    private transient final Socket socket;
    private transient final PrintWriter printer;

    public TCPWriter(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.printer = new PrintWriter(socket.getOutputStream(), true);
    }

    public void println(String str) {
        printer.println(str);
    }

    public void close() {
        try {
            printer.flush();
            printer.close();
            socket.close();
        } catch (IOException e) {
            throw new AssertionError("Unexpected error", e);
        }
    }
}