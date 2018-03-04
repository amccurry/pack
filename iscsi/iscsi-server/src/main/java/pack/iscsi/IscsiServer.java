package pack.iscsi;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.concurrent.Executors;

public class IscsiServer implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IscsiServer.class);

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private final Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();
  private Timer _timer;

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      _targetServers.put(address,
          new TargetServer(InetAddress.getByName(address), config.getPort(), config.getIscsiTargetManager()));
    }
    _executorService = Executors.newCachedThreadPool("iscsiserver");
  }

  public void registerTargets() throws Exception {
    LOGGER.debug("Registering targets");
  }

  public void start() {
    for (Entry<String, TargetServer> e : _targetServers.entrySet()) {
      _futures.put(e.getKey(), _executorService.submit(e.getValue()));
    }
    _timer = new Timer("register volumes", true);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          registerTargets();
        } catch (Throwable t) {
          LOGGER.error("Unknown error");
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
