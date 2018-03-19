package pack.distributed.storage.http;

import static spark.Spark.*;
import static spark.Spark.port;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletOutputStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import spark.ModelAndView;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class HttpServer {

  private static final String WEB = "/web";

  public static void main(String[] args) {
    AtomicReference<byte[]> textMetricsOutput = new AtomicReference<byte[]>("stats".getBytes());
    ImmutableList<CompactorServerInfo> compactors = ImmutableList.of(CompactorServerInfo.builder()
                                                                                        .address("address1")
                                                                                        .hostname("hostname1")
                                                                                        .build());
    ImmutableList<TargetServerInfo> targets = ImmutableList.of(TargetServerInfo.builder()
                                                                               .address("address1")
                                                                               .hostname("hostname1")
                                                                               .bindAddress("bindAddress1")
                                                                               .build());

    ImmutableList<Volume> volumes = ImmutableList.of(Volume.builder()
                                                           .hdfsPath("/path/test")
                                                           .iqn("iqn.1234")
                                                           .kafkaTopic("pack.test.2341234123")
                                                           .name("test")
                                                           .size(10000000000L)
                                                           .build());

    InfoLookup infoLookup = new InfoLookup() {

      @Override
      public List<Volume> getVolumes() {
        return volumes;
      }

      @Override
      public List<TargetServerInfo> getTargets() {
        return targets;
      }

      @Override
      public List<CompactorServerInfo> getCompactors() {
        return compactors;
      }
    };
    HttpServerConfig config = HttpServerConfig.builder()
                                              .infoLookup(infoLookup)
                                              .port(8642)
                                              .textMetricsOutput(textMetricsOutput)
                                              .build();

    startHttpServer(config);
  }

  public static void startHttpServer(HttpServerConfig httpServerConfig) {
    port(httpServerConfig.getPort());

    InfoLookup infoLookup = httpServerConfig.getInfoLookup();

    Configuration config = new Configuration(Configuration.VERSION_2_3_26);
    config.setTemplateLoader(new ClassTemplateLoader(HttpServer.class, WEB));
    FreeMarkerEngine engine = new FreeMarkerEngine(config);

    staticFileLocation(WEB);

    TemplateViewRoute overview = (TemplateViewRoute) (request, response) -> {
      List<TargetServerInfo> targets = infoLookup.getTargets();
      List<CompactorServerInfo> compactors = infoLookup.getCompactors();
      return new ModelAndView(ImmutableMap.of("targets", targets, "compactors", compactors), "index.ftl");
    };
    get("/", overview, engine);
    get("/index.html", overview, engine);
    get("/sessions.html", (request, response) -> {
      Map<String, String> attributes = new HashMap<>();
      attributes.put("test", "cool");
      return new ModelAndView(attributes, "sessions.ftl");
    }, engine);
    get("/volumes.html", (request, response) -> {
      List<Volume> volumes = infoLookup.getVolumes();
      return new ModelAndView(ImmutableMap.of("volumes", volumes), "volumes.ftl");
    }, engine);
    get("/metrics/text", new Route() {
      @Override
      public Object handle(Request request, Response response) throws Exception {
        response.type("text/plain");
        ServletOutputStream outputStream = response.raw()
                                                   .getOutputStream();
        outputStream.write(httpServerConfig.getTextMetricsOutput()
                                           .get());
        outputStream.flush();
        return "";
      }
    });

  }

}
