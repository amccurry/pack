package pack.distributed.storage.http;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletOutputStream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import pack.distributed.storage.metrics.json.JsonReporter;
import pack.distributed.storage.metrics.json.SetupJvmMetrics;
import pack.iscsi.storage.utils.PackUtils;
import spark.ModelAndView;
import spark.Service;
import spark.TemplateViewRoute;
import spark.template.freemarker.FreeMarkerEngine;

public class HttpServer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String WEB = "/web";

  public static void main(String[] args) {

    MetricRegistry registry = new MetricRegistry();
    JsonReporter jsonReporter = new JsonReporter(registry);
    jsonReporter.start(3, TimeUnit.SECONDS);
    PackUtils.closeOnShutdown(jsonReporter);

    SetupJvmMetrics.setup(registry);

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

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        registry.gauge("g1", () -> () -> UUID.randomUUID()
                                             .toString());
        Random random = new Random();
        while (true) {
          Counter counter = registry.counter("c1");
          counter.inc(random.nextInt(100000));
          Meter meter = registry.meter("m1");
          meter.mark(random.nextInt(100000));
          Histogram histogram = registry.histogram("h1");
          histogram.update(random.nextInt(100000));
          Timer timer = registry.timer("t1");
          timer.update(random.nextInt(100000), TimeUnit.NANOSECONDS);
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            return;
          }
        }

      }
    });
    thread.setDaemon(true);
    thread.start();

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

      @Override
      public List<Metric> getMetrics() {
        return Metric.flatten(jsonReporter.getReport());
      }

      @Override
      public List<Session> getSessions() {
        return ImmutableList.of();
      }
    };
    HttpServerConfig config = HttpServerConfig.builder()
                                              .packDao(packDao)
                                              .port(8642)
                                              .textMetricsOutput(textMetricsOutput)
                                              .build();

    startHttpServer(config, new JsonReporter(new MetricRegistry()));
  }

  public static Service startHttpServer(HttpServerConfig httpServerConfig, JsonReporter jsonReporter) {
    Service service = Service.ignite();

    service.ipAddress(httpServerConfig.getAddress());
    service.port(httpServerConfig.getPort());

    PackDao dao = httpServerConfig.getPackDao();

    Configuration config = new Configuration(Configuration.VERSION_2_3_26);
    config.setTemplateLoader(new ClassTemplateLoader(HttpServer.class, WEB));
    FreeMarkerEngine engine = new FreeMarkerEngine(config);

    service.staticFileLocation(WEB);

    TemplateViewRoute overview = (TemplateViewRoute) (request, response) -> {
      List<TargetServerInfo> targets = dao.getTargets();
      List<CompactorServerInfo> compactors = dao.getCompactors();
      return new ModelAndView(ImmutableMap.of("targets", targets, "compactors", compactors), "index.ftl");
    };
    service.get("/", overview, engine);
    service.get("/index.html", overview, engine);
    service.get("/sessions.html", (request, response) -> {
      List<Session> sessions = dao.getSessions();
      return new ModelAndView(ImmutableMap.of("sessions", sessions), "sessions.ftl");
    }, engine);
    service.get("/volumes.html", (request, response) -> {
      List<Volume> volumes = dao.getVolumes();
      return new ModelAndView(ImmutableMap.of("volumes", volumes), "volumes.ftl");
    }, engine);
    service.get("/metrics.html", (request, response) -> {
      List<Metric> metrics = dao.getMetrics();
      return new ModelAndView(ImmutableMap.of("metrics", metrics), "metrics.ftl");
    }, engine);
    service.get("/metrics/text", (request, response) -> {
      response.type("text/plain");
      ServletOutputStream outputStream = response.raw()
                                                 .getOutputStream();
      outputStream.write(httpServerConfig.getTextMetricsOutput()
                                         .get());
      outputStream.flush();
      return "";
    });
    service.get("/metrics/json", (request, response) -> {
      response.type("application/javascript");
      ServletOutputStream outputStream = response.raw()
                                                 .getOutputStream();
      MAPPER.writeValue(outputStream, jsonReporter.getReport());
      outputStream.flush();

      return "";
    });

    return service;

  }

}
