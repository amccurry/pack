package pack.iscsi.server;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.jscsi.target.TargetServer;

import pack.iscsi.storage.utils.Executors;

public class IscsiServer implements Closeable {

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private final Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();
  private final Timer _timer = new Timer("register volumes", true);

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      _targetServers.put(address,
          new TargetServer(InetAddress.getByName(address), config.getPort(), config.getIscsiTargetManager()));
    }
    _executorService = Executors.newCachedThreadPool("iscsiserver");
  }

  public void start() {
    for (Entry<String, TargetServer> e : _targetServers.entrySet()) {
      _futures.put(e.getKey(), _executorService.submit(e.getValue()));
    }
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
