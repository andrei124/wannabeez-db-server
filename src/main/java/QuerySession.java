import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QuerySession {

  private static final int SERVER_PORT = 11234;
  private static final int NUMBER_OF_THREADS = 2;

  public static void main(String[] args) throws SQLException {

    System.out.println("SERVER STARTED...");

    ExecutorService executorService = null;

    try {
      ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
      executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("A client connected !!!!!!!");
        Runnable command = new RequestProcessor(clientSocket);
        executorService.execute(command);
      }
    } catch (IOException e) {
      System.out.println("Encountered exception when trying to listen for connections");
      System.out.println(e.getMessage());
      e.printStackTrace();
    } finally {
      if (executorService != null) {
        executorService.shutdown();
      }
    }
  }
}
