import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import org.json.*;
import com.sun.net.httpserver.HttpExchange;

public class RequestProcessor implements Runnable {

  private final Socket clientSocket;
  private final QueryProcessor queryProcessor;
  private final HttpExchange exchange;

  public RequestProcessor(Socket clientSocket, QueryProcessor queryProcessor, HttpExchange exchange) {
    this.clientSocket = clientSocket;
    this.queryProcessor = queryProcessor;
    this.exchange = exchange;
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
        parseClientInput(clientInput);
        String result = "Result";
        writer.println(result);
      }
    } catch (IOException e) {
      System.out.println("IO Exception occurred when trying to open Input or Output stream");
      System.out.println(e.getMessage());
    }
  }

  private void parseClientInput(String clientInput) {
    System.out.println("Client input: " + clientInput);
    try {
      JSONObject obj = new JSONObject(clientInput);
      int id = obj.getInt("id");
      System.out.println("Item retrieved: id = " + id);
    } catch (JSONException e) {
      System.out.println("Parsed string is not json format");
      e.printStackTrace();
    }
  }
}
