import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TestClient {
  private final String serverIP;

  public TestClient(String serverIP) {
    this.serverIP = serverIP;
  }

  public void connectToServer() throws IOException {
    Socket socket = new Socket(serverIP, QuerySession.SERVER_PORT);
    if (socket.isConnected()) {
      sendJsonObject(socket);
      System.out.println("It connected");
    } else {
      System.out.println("It didn't connect");
    }
  }

  private void sendJsonObject(Socket serverSocket) {
    try {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
      PrintWriter writer = new PrintWriter(serverSocket.getOutputStream(), true);

      System.out.println("I am trying to send a message.");
      writer.println("message");
      System.out.println(reader.readLine());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    new TestClient("placeholder").connectToServer();
  }
}
