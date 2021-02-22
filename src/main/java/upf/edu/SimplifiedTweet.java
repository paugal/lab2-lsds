package upf.edu;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Optional;

public class SimplifiedTweet {

  /*private static ?? parser = new ????;*/

  //Este Json nos sirve para parsear
  private static JsonParser parser = new JsonParser();

  private final Long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final Long userId;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final Long timestampMs;		  // seconds from epoch ('timestamp_ms')

  public SimplifiedTweet(Long tweetId, String text, Long userId, String userName,
                         String language, Long timestampMs) {
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
    this.timestampMs = timestampMs;
  }

  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Optional<SimplifiedTweet> fromJson(String jsonStr) {
    Long tweetId = null, userId = null, timestampMs = null;
    String text = null, userName = null, language= null;



    JsonElement json_aux = SimplifiedTweet.parser.parse(jsonStr);
    JsonObject final_tweet = json_aux.getAsJsonObject();
    //System.out.println(final_tweet);

    try {
      Optional<Long> oTweetId = Optional.ofNullable(final_tweet.get("id").getAsLong());
      //System.out.println("TweetId: " + oTweetId);
      if(oTweetId.isPresent()){
        tweetId = final_tweet.get("id").getAsLong();
      }else
        return Optional.empty();
    }catch(Exception e) {
      return Optional.empty();
    }


    Optional <String> oText = Optional.ofNullable(final_tweet.get("text").getAsString());
    if(oText.isPresent()){
      text = final_tweet.get("text").getAsString();
    }else
      return Optional.empty();

    Optional <String> oLanguage = Optional.ofNullable(final_tweet.get("lang").getAsString().replace("\n", " "));
    if(oLanguage.isPresent()){
      language = final_tweet.get("lang").getAsString().replace("\n", " ");
    }else
      return Optional.empty();

    Optional <Long> oTimestampMs = Optional.ofNullable(final_tweet.get("timestamp_ms").getAsLong());
    if(oTimestampMs.isPresent()){
      timestampMs = final_tweet.get("timestamp_ms").getAsLong();
    }else
      return Optional.empty();

    Optional <JsonObject> oUser = Optional.ofNullable(final_tweet.get("user").getAsJsonObject());
    if(oUser.isPresent()){
      JsonObject user = final_tweet.get("user").getAsJsonObject();
      Optional <Long> oUserId = Optional.ofNullable(user.get("id").getAsLong());
      if (oUserId.isPresent()){
        userId = user.get("id").getAsLong();
      }else{
        return Optional.empty();
      }
      Optional <String> oUserName = Optional.ofNullable(user.get("name").getAsString());
      if(oUserName.isPresent()){
        userName = user.get("name").getAsString();
      }else{
        return Optional.empty();
      }

    }else
      return Optional.empty();

    /*
    //Miramos que el json tenga todos los elementos y lo guardamos como simplified tweet
    if(final_tweet.has("id")) {
      tweetId = final_tweet.get("id").getAsLong();
      tweetId = null;
      System.out.println("TweetId: " + tweetId);
    }

    if(final_tweet.has("text")) {
      text = final_tweet.get("text").getAsString();
      //System.out.println(text);
    }

     if (final_tweet.has("lang")) {
       language = final_tweet.get("lang").getAsString().replace("\n", " ");
       //System.out.println(language);
     }

     if (final_tweet.has("timestamp_ms")){
      timestampMs = final_tweet.get("timestamp_ms").getAsLong();
      //System.out.println(timestampMs);
    }
    if(final_tweet.has("user"))
    {
      JsonObject user = final_tweet.get("user").getAsJsonObject();
      if(user.has("id")) {
        userId = user.get("id").getAsLong();
        //System.out.println(userId);
      }
      if(user.has("name")){
        userName = user.get("name").getAsString();
        //System.out.println(userName);
      }

    }

    */


    SimplifiedTweet simplified_tweet = new SimplifiedTweet(tweetId, text, userId, userName, language, timestampMs);

    return Optional.ofNullable(simplified_tweet);


  }

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }

  public long getTweetId() {
    return tweetId;
  }

  public String getText() {
    return text;
  }

  public long getUserId() {
    return userId;
  }

  public String getUserName() {
    return userName;
  }

  public String getLanguage() {
    return language;
  }

  public long getTimestampMs() {
    return timestampMs;
  }
}