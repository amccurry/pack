package pack.iscsi.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.file.FileStorageModule;
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;
import pack.iscsi.spi.StorageModuleFactory;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  public static void main(String[] args) throws Exception {
    Set<String> addresses = new HashSet<String>();
    addresses.add("127.0.0.3");
    List<StorageModuleFactory> factories = new ArrayList<>();
    factories.add(FileStorageModule.createFactory(new File("./volume")));
    TargetManager targetManager = new BaseTargetManager(factories);
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .port(3260)
                                                .iscsiTargetManager(targetManager)
                                                .build();
    runServer(config);
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
