import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import org.json.JSONException;

public class RequestProcessor implements Runnable {

  private final Socket clientSocket;
  private final QueryProcessor queryProcessor;

  public RequestProcessor(Socket clientSocket, QueryProcessor queryProcessor) {
    this.clientSocket = clientSocket;
    this.queryProcessor = queryProcessor;
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
        ImageObject obj = parseClientInput(clientInput);
        String result;
        if (insertImageMetaDataIntoDB(obj)) {
          result = "Image Received";
        } else {
          result = "Something went wrong";
        }
        writer.println(result);
      }
    } catch (IOException | JSONException e) {
      System.out.println("IO Exception occurred when trying to open Input or Output stream");
      System.out.println(e.getMessage());
    }
  }

  private ImageObject parseClientInput(String clientInput) throws JSONException {
    return new ImageObject(clientInput);
  }

  public boolean insertImageMetaDataIntoDB(ImageObject obj) {
    queryProcessor.connect();

    try {
      queryProcessor.insertIntoGallery(obj.getTimestamp(), obj.getPlayerId(), obj.getUrl());
    } catch (SQLException throwables) {
      throwables.printStackTrace();
      return false;
    } finally {
      queryProcessor.closeConnection();
    }
    return true;
  }
}
