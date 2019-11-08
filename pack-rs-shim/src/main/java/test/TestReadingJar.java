package test;

import java.io.IOException;
import java.io.InputStream;

public class TestReadingJar {

  public static void main(String[] args) throws IOException {
    try (InputStream inputStream = TestReadingJar.class.getResourceAsStream("/pack-rs-lib/accessors-smart-1.2.jar")) {
      System.out.println(inputStream);
    }
  }

}
