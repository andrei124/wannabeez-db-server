import java.sql.Timestamp;
import org.json.JSONException;
import org.json.JSONObject;

public class ImageObject {
  private final int playerId;
  private final int imageId;
  private final Timestamp timestamp;
  private final String url;

  public ImageObject(String imageObjectAsJSON) throws JSONException {
    JSONObject obj = new JSONObject(imageObjectAsJSON);
    this.playerId = obj.getInt("playerId");
    this.imageId = obj.getInt("imageId");
    this.url = obj.getString("url");
    this.timestamp = Timestamp.valueOf(obj.getString("timestamp"));
  }

  public ImageObject(int playerId, int imageId, Timestamp timestamp, String url) {
    this.playerId = playerId;
    this.imageId = imageId;
    this.timestamp = timestamp;
    this.url = url;
  }

  public int getPlayerId() {
    return playerId;
  }

  public int getImageId() {
    return imageId;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }

  public String getUrl() {
    return url;
  }
}
