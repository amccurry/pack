package pack.iscsi.http;

import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletOutputStream;

import spark.Request;
import spark.Response;
import spark.Route;

import static spark.Spark.*;

public class HttpServer {

  public static void startHttpServer(AtomicReference<byte[]> metricsOutput) {
    port(8642);
    get("/metrics-text", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        response.type("text/plain");
        ServletOutputStream outputStream = response.raw()
                                                   .getOutputStream();
        outputStream.write(metricsOutput.get());
        outputStream.flush();
        return null;
      }
    });

  }

}
