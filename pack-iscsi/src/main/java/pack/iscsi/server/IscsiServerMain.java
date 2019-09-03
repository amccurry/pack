package pack.iscsi.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactory;
import pack.iscsi.partitioned.storagemanager.BlockStorageModuleFactoryConfig;
import pack.iscsi.spi.StorageModuleFactory;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    CommandLine cmd = IscsiServerArgsUtil.parseArgs(args);
    Set<String> addresses = IscsiServerArgsUtil.getAddresses(cmd);
    int port = IscsiServerArgsUtil.getPort(cmd);
    String configDir = IscsiServerArgsUtil.getConfigDir(cmd);

    try (Closer closer = Closer.create()) {
      List<StorageModuleFactory> factories = new ArrayList<>();
      List<BlockStorageModuleFactoryConfig> configs = IscsiConfigUtil.getConfigs(new File(configDir));
      for (BlockStorageModuleFactoryConfig config : configs) {
        factories.add(closer.register(new BlockStorageModuleFactory(config)));
      }
      TargetManager targetManager = new BaseTargetManager(factories);
      IscsiServerConfig config = IscsiServerConfig.builder()
                                                  .addresses(addresses)
                                                  .port(port)
                                                  .iscsiTargetManager(targetManager)
                                                  .build();
      runServer(config);
    }
  }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      LOGGER.info("Starting server");
      iscsiServer.start();
      LOGGER.info("Server started");
      iscsiServer.join();
    }
  }

}
