import java.sql.Timestamp;
import org.json.JSONException;
import org.json.JSONObject;

public class ImageObject {
  private final int userId;
  private final int imageId;
  private final Timestamp timestamp;
  private final String url;

  public ImageObject(String imageObjectAsJSON) throws JSONException {
    JSONObject obj = new JSONObject(imageObjectAsJSON);
    this.userId = obj.getInt("userId");
    this.imageId = obj.getInt("imageId");
    this.url = obj.getString("url");
    this.timestamp = Timestamp.valueOf(obj.getString("timestamp"));
  }

  public ImageObject(int userId, int imageId, Timestamp timestamp, String url) {
    this.userId = userId;
    this.imageId = imageId;
    this.timestamp = timestamp;
    this.url = url;
  }

  public int getUserId() {
    return userId;
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
