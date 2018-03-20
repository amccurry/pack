package pack.distributed.storage.http;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.staticFileLocation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletOutputStream;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import pack.distributed.storage.metrics.json.JsonReporter;
import spark.ModelAndView;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class HttpServer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
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

    PackDao packDao = new PackDao() {

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
                                              .packDao(packDao)
                                              .port(8642)
                                              .textMetricsOutput(textMetricsOutput)
                                              .build();

    startHttpServer(config, new JsonReporter(new MetricRegistry()));
  }

  public static void startHttpServer(HttpServerConfig httpServerConfig, JsonReporter jsonReporter) {
    port(httpServerConfig.getPort());

    PackDao dao = httpServerConfig.getPackDao();

    Configuration config = new Configuration(Configuration.VERSION_2_3_26);
    config.setTemplateLoader(new ClassTemplateLoader(HttpServer.class, WEB));
    FreeMarkerEngine engine = new FreeMarkerEngine(config);

    staticFileLocation(WEB);

    TemplateViewRoute overview = (TemplateViewRoute) (request, response) -> {
      List<TargetServerInfo> targets = dao.getTargets();
      List<CompactorServerInfo> compactors = dao.getCompactors();
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
      List<Volume> volumes = dao.getVolumes();
      return new ModelAndView(ImmutableMap.of("volumes", volumes), "volumes.ftl");
    }, engine);
    get("/metrics/text", (request, response) -> {
      response.type("text/plain");
      ServletOutputStream outputStream = response.raw()
                                                 .getOutputStream();
      outputStream.write(httpServerConfig.getTextMetricsOutput()
                                         .get());
      outputStream.flush();
      return "";
    });
    get("/metrics/json", (request, response) -> {
      response.type("application/javascript");
      ServletOutputStream outputStream = response.raw()
                                                 .getOutputStream();
      MAPPER.writeValue(outputStream, jsonReporter.getReport());
      outputStream.flush();
      
      return "";
    });
    

  }

}
