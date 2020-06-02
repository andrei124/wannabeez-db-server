import java.io.IOException;
import java.net.Socket;

public class TestClient {
  private final String serverIP;

  public TestClient(String serverIP) {
    this.serverIP = serverIP;
  }

  public void connectToServer() throws IOException {
    Socket socket = new Socket(serverIP, QuerySession.SERVER_PORT);
    if (socket.isConnected()) {
      System.out.println("It connected");
    } else {
      System.out.println("It didn't connect");
    }
  }

  public static void main(String[] args) throws IOException {
    new TestClient("SWAP_ME_WITH_REAL_IP_ADDR").connectToServer();
  }
}
