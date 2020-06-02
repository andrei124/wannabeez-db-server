import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class RequestProcessor implements Runnable {

  private final Socket clientSocket;
  private QueryProcessor queryProcessor;

  public RequestProcessor(Socket clientSocket) {
    this.clientSocket = clientSocket;
    this.queryProcessor = new QueryProcessor();
  }

  @Override
  public void run() {
    try {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);

      System.out.println("Thread currently executing: " + Thread.currentThread().getName());
      String clientInput;

      while ((clientInput = reader.readLine()) != null) {
        System.out.println(clientInput);
        String result = "Result";
        writer.println(result);
      }
    } catch (IOException e) {
      System.out.println("IO Exception occurred when trying to open Input or Output stream");
      System.out.println(e.getMessage());
    }
  }
}
