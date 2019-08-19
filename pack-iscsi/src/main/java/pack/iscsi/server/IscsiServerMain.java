package pack.iscsi.server;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.file.FileStorageModule;
import pack.iscsi.file.FileStorageModule.FileStorageModuleFactory;
import pack.iscsi.manager.BaseTargetManager;
import pack.iscsi.manager.TargetManager;

public class IscsiServerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServerMain.class);

  private static final String PACK = "pack";
  private static final String _2018_02 = "2018-02";

  public static void main(String[] args) throws Exception {
    Set<String> addresses = new HashSet<String>();
    addresses.add("127.0.0.3");
    String cache = "./cache";
    File cacheDir = new File(cache);
    TargetManager iscsiTargetManager = new BaseTargetManager(_2018_02, PACK);
    FileStorageModuleFactory factory = FileStorageModule.createFactory(new File("./volume"));
    IscsiServerConfig config = IscsiServerConfig.builder()
                                                .addresses(addresses)
                                                .port(3260)
                                                .cacheDir(cacheDir)
                                                .iscsiTargetManager(iscsiTargetManager)
                                                .iStorageModuleFactory(factory)
                                                .build();
    runServer(config);
  }

  public static void runServer(IscsiServerConfig config) throws IOException, InterruptedException, ExecutionException {
    try (IscsiServer iscsiServer = new IscsiServer(config)) {
      iscsiServer.registerTargets();
      LOGGER.info("Starting server");
      iscsiServer.start();
      LOGGER.info("Server started");
      iscsiServer.join();
    }
  }

}
