package pack.block.classpath;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetPackHadoopClasspath {

  private static final String SEP = ":";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String COM_SUN_JERSEY_JERSEY_SERVER = "com/sun/jersey/jersey-server";
  private static final String COM_SUN_JERSEY_JERSEY_CORE = "com/sun/jersey/jersey-core";
  private static final String ORG_MORTBAY_JETTY_JETTY_UTIL = "org/mortbay/jetty/jetty-util";
  private static final String COM_SUN_JERSEY_JERSEY_JSON = "com/sun/jersey/jersey-json";
  private static final String ORG_MORTBAY_JETTY_JETTY = "org/mortbay/jetty/jetty";
  private static final String JAVAX_SERVLET_JAVAX_SERVLET_API = "javax/servlet/javax.servlet-api";

  public static void main(String[] args) throws IOException {
    String cp = System.getProperty(JAVA_CLASS_PATH);
    String[] strings = cp.split(SEP);
    String[] removeArray = new String[] { JAVAX_SERVLET_JAVAX_SERVLET_API, ORG_MORTBAY_JETTY_JETTY,
        ORG_MORTBAY_JETTY_JETTY_UTIL, COM_SUN_JERSEY_JERSEY_CORE, COM_SUN_JERSEY_JERSEY_JSON,
        COM_SUN_JERSEY_JERSEY_SERVER };

    List<String> result = new ArrayList<>();
    OUTER: for (String s : strings) {
      for (String r : removeArray) {
        if (!s.contains(r)) {
          if (!result.contains(s)) {
            result.add(new File(s).getCanonicalPath());
            continue OUTER;
          }
        }
      }
    }

    boolean sep = false;
    for (String s : result) {
      if (sep) {
        System.out.print(SEP);
      }
      System.out.print(s);
      sep = true;
    }
    System.out.println();
  }

}
