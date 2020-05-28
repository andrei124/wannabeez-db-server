public class QuerySession {

  public static void main(String[] args) {

      QueryProcessor queryProcessor = new QueryProcessor();
      queryProcessor.connect();
      queryProcessor.closeConnection();
  }
}
