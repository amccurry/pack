package pack.iscsi;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.*;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import pack.iscsi.http.HttpServer;
import pack.iscsi.metrics.ConsoleReporter;
import pack.iscsi.metrics.MetricsStorageTargetManager;
import pack.iscsi.metrics.PrintStreamFactory;
import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.StorageTargetManagerFactory;
import pack.iscsi.storage.utils.PackUtils;

public class IscsiServerMain {

  private static final String PACK_ISCSI_ADDRESS = "PACK_ISCSI_ADDRESS";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    PackUtils.setupLog4j();
    List<String> addresses = PackUtils.getEnvListFailIfMissing(PACK_ISCSI_ADDRESS);

    MetricRegistry registry = new MetricRegistry();

    AtomicReference<byte[]> metricsOutput = new AtomicReference<>();
    setupReporter(registry, metricsOutput);
    HttpServer.startHttpServer(metricsOutput);

    List<StorageTargetManager> targetManagers = new ArrayList<>();
    ServiceLoader<StorageTargetManagerFactory> loader = ServiceLoader.load(StorageTargetManagerFactory.class);
    for (StorageTargetManagerFactory factory : loader) {
      LOGGER.info("Loading factory {} {}", factory.getClass(), factory);
      StorageTargetManager manager = factory.create();
      targetManagers.add(manager);
    }

    StorageTargetManager manager = MetricsStorageTargetManager.wrap(registry,
        StorageTargetManager.merge(targetManagers));
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(ImmutableSet.copyOf(addresses))
                                                .port(3260)
                                                .iscsiTargetManager(manager)
                                                .build();
    runServer(config);
  }

  private static void setupReporter(MetricRegistry registry, AtomicReference<byte[]> ref) {
    PrintStreamFactory printStreamFactory = () -> {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      return new PrintStream(outputStream) {
        @Override
        public void close() {
          super.close();
          ref.set(outputStream.toByteArray());
        }
      };
    };
    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
                                              .convertRatesTo(TimeUnit.SECONDS)
                                              .convertDurationsTo(TimeUnit.MILLISECONDS)
                                              .outputTo(printStreamFactory)
                                              .build();
    reporter.start(3, TimeUnit.SECONDS);
  }

  public static void runServer(IscsiServerConfig config) throws Exception {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      iscsiServer.start();
      iscsiServer.join();
    }
  }

}
