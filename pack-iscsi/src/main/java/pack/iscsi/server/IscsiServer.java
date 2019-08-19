package pack.iscsi.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jscsi.target.TargetServer;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.manager.TargetManager;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.iscsi.storage.utils.Executors;
import pack.iscsi.storage.utils.WrapperIStorageModule;

public class IscsiServer implements Closeable {

  private static final String PACK = "pack";

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private final Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();
  private final TargetManager _iscsiTargetManager;
  private final Timer _timer = new Timer("register volumes", true);
  private final StorageModuleFactory _storageModuleFactory;

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      _targetServers.put(address,
          new TargetServer(InetAddress.getByName(address), config.getPort(), config.getIscsiTargetManager()));
    }
    _executorService = Executors.newCachedThreadPool("iscsiserver");
    _iscsiTargetManager = config.getIscsiTargetManager();
    _storageModuleFactory = config.getIStorageModuleFactory();
  }

  public void registerTargets() throws IOException, InterruptedException, ExecutionException {
    LOGGER.debug("Registering targets");
    List<String> names = _storageModuleFactory.getStorageModuleNames();
    for (String name : names) {
      if (!_iscsiTargetManager.isValidTarget(_iscsiTargetManager.getFullName(name))) {
        LOGGER.info("Registering target {}", name);
        StorageModule storageModule = _storageModuleFactory.getStorageModule(name);
        if (storageModule != null) {
          _iscsiTargetManager.register(name, PACK + " " + name, toIStorageModule(storageModule));
        }
      }
    }
  }

  private IStorageModule toIStorageModule(StorageModule storageModule) {
    return WrapperIStorageModule.create(storageModule);
  }

  public void start() {
    for (Entry<String, TargetServer> e : _targetServers.entrySet()) {
      _futures.put(e.getKey(), _executorService.submit(e.getValue()));
    }
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          registerTargets();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    }, TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10));
  }

  public void join() throws InterruptedException, ExecutionException {
    for (Future<Void> future : _futures.values()) {
      future.get();
    }
  }

  @Override
  public void close() throws IOException {
    _timer.purge();
    _timer.cancel();
    for (Future<Void> future : _futures.values()) {
      future.cancel(true);
    }
    _executorService.shutdownNow();
  }
}
