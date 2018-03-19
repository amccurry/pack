package pack.distributed.storage.http;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.put;
import static spark.Spark.staticFileLocation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletOutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import pack.distributed.storage.PackMetaData;
import pack.iscsi.storage.utils.PackUtils;
import spark.ModelAndView;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class HttpServer {

  private static final String NAME = "name";
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

    startHttpServer(config);
  }

  public static void startHttpServer(HttpServerConfig httpServerConfig) {
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
    put("/api/create/:name", (request, response) -> {
      String name = request.params(NAME);
      PackMetaData metaData = MAPPER.readValue(request.body(), PackMetaData.class);

      if (metaData.getSerialId() == null) {
        String serialId = PackUtils.generateSerialId()
                                   .toString();
        metaData = metaData.toBuilder()
                           .serialId(serialId)
                           .build();
      }

      if (metaData.getTopicId() == null) {
        String newTopicId = PackUtils.getTopic(name, UUID.randomUUID()
                                                         .toString());
        metaData = metaData.toBuilder()
                           .topicId(newTopicId)
                           .build();

      }

      dao.createVolume(name, metaData);
      return "{}";
    });
    get("/api/list", (request, response) -> {
      List<Volume> volumes = dao.getVolumes();
      return MAPPER.writeValueAsString(volumes);
    });

  }

}
