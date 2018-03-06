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
  private static final String PACK_ISCSI_SERIAL_ID = "PACK_ISCSI_SERIAL_ID";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    List<String> addresses = PackUtils.getEnvListFailIfMissing(PACK_ISCSI_ADDRESS);
    String serialid = PackUtils.getEnvFailIfMissing(PACK_ISCSI_SERIAL_ID);

    List<StorageTargetManager> targetManagers = new ArrayList<>();
    ServiceLoader<StorageTargetManagerFactory> loader = ServiceLoader.load(StorageTargetManagerFactory.class);
    for (StorageTargetManagerFactory factory : loader) {
      LOGGER.info("Loading factory {} {}", factory.getClass(), factory);
      targetManagers.add(factory.create());
    }

    StorageTargetManager manager = StorageTargetManager.merge(targetManagers);
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(ImmutableSet.copyOf(addresses))
                                                .port(3260)
                                                .iscsiTargetManager(manager)
                                                .serialId(serialid)
                                                .build();
    runServer(config);
  }

  public static void runServer(IscsiServerConfig config) throws Exception {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      iscsiServer.registerTargets();
      iscsiServer.start();
      iscsiServer.join();
    }
  }

}