import java.security.MessageDigest;
import java.math.BigInteger;
import java.io.*;

public class KeyToToken {
  public static void main(String[] args) throws Exception {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = reader.readLine()) != null) {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(line.getBytes());
      System.out.println(new BigInteger(md.digest()).abs().toString());
    }
  }
}
