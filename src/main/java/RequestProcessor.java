import java.io.*;
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
    BufferedReader reader;
    BufferedWriter writer;

    try {
      reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));

      System.out.println("Thread currently executing: " + Thread.currentThread().getName());
      String clientInput;

      while ((clientInput = reader.readLine()) != null) {
        /** TODO: Code to handle request */
        String result = "Result";
        writer.write(result);
      }
    } catch (IOException e) {
      System.out.println("IO Exception occurred when trying to open Input or Output stream");
      System.out.println(e.getMessage());
    }
  }
}
