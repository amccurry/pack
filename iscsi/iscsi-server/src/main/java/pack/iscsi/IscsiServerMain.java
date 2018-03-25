package pack.iscsi;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import pack.iscsi.storage.StorageTargetManager;
import pack.iscsi.storage.StorageTargetManagerFactory;
import pack.iscsi.storage.utils.PackUtils;

public class IscsiServerMain {

  private static final String PACK_ISCSI_ADDRESS = "PACK_ISCSI_ADDRESS";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    PackUtils.setupLog4j();
    PackUtils.bootStrapProtpertyFile();
    List<String> addresses = PackUtils.getPropertyListFailIfMissing(PACK_ISCSI_ADDRESS);

    List<StorageTargetManager> targetManagers = new ArrayList<>();
    ServiceLoader<StorageTargetManagerFactory> loader = ServiceLoader.load(StorageTargetManagerFactory.class);
    for (StorageTargetManagerFactory factory : loader) {
      LOGGER.info("Loading factory {} {}", factory.getClass(), factory);
      try {
        StorageTargetManager manager = factory.create();
        targetManagers.add(manager);
      } catch (Throwable t) {
        LOGGER.error("Could not create manager from factory {} error {}", factory.getClass(), t.getMessage());
      }
    }

    StorageTargetManager manager = StorageTargetManager.merge(targetManagers);
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(ImmutableSet.copyOf(addresses))
                                                .port(3260)
                                                .iscsiTargetManager(manager)
                                                .build();
    runServer(config);
  }

  public static void runServer(IscsiServerConfig config) throws Exception {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      LOGGER.info("Starting Iscsi Server on port {}", config.getPort());
      iscsiServer.start();
      iscsiServer.join();
    }
  }

}
