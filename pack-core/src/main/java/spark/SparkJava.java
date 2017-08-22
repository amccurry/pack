package spark;

import java.lang.reflect.Field;

import spark.embeddedserver.EmbeddedServers;
import spark.embeddedserver.jetty.UnixSocketEmbeddedJettyFactory;

public class SparkJava {

  public static void init() {
    EmbeddedServers.add(SparkJavaIdentifier.UNIX_SOCKET, new UnixSocketEmbeddedJettyFactory());
  }

  public static void main(String[] args) {
    Service service = Service.ignite();
    configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    service.ipAddress("/tmp/testing");
    service.get("/", (request, response) -> "hello world");
  }

  public static void configureService(Object identifier, Service service) {
    try {
      Field field = Service.class.getDeclaredField("embeddedServerIdentifier");
      field.setAccessible(true);
      field.set(service, identifier);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
