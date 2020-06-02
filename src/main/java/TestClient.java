import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TestClient {
  private final String serverIP;
  private Socket socket = null;

  public TestClient(String serverIP) {
    this.serverIP = serverIP;
  }

  public void connectToServer() throws IOException {
    socket = new Socket(serverIP, QuerySession.SERVER_PORT);
    if (socket.isConnected()) {
      System.out.println("It connected");
    } else {
      System.out.println("It didn't connect");
    }
  }

  private void sendJsonObject(String jsonObject) {
    try {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(socket.getInputStream()));
      PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

      System.out.println("I am trying to send a message.");
      writer.println(jsonObject);
      System.out.println(reader.readLine());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    TestClient client = new TestClient("placehoooder");
    client.connectToServer();
    client.sendJsonObject("{"
        + "\"id\": 123"
        + "}");
  }
}
