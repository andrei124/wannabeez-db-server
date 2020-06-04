
import java.io.IOException;
import java.sql.SQLException;

public class QuerySession {

  public static final int PORT = 8500;

  public static void main(String[] args) throws IOException {
    Server server = new Server(PORT, new QueryProcessor(), new ImageProcessor());
    server.start();
  }

}
