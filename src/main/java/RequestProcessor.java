import java.net.Socket;

public class RequestProcessor implements Runnable {

    private Socket clientSocket;
    private QueryProcessor queryProcessor;

    public RequestProcessor(Socket clientSocket) {
        this.clientSocket = clientSocket;
        queryProcessor = new QueryProcessor();
    }

    @Override
    public void run() {

    }
}
