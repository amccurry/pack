package pack.iscsi;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.jscsi.target.TargetServer;

import com.google.common.base.Splitter;

import pack.iscsi.storage.concurrent.Executors;

public class IscsiServer implements Closeable {

  private final Map<String, TargetServer> _targetServers = new ConcurrentHashMap<>();
  private final ExecutorService _executorService;
  private final Map<String, Future<Void>> _futures = new ConcurrentHashMap<>();

  public IscsiServer(IscsiServerConfig config) throws IOException {
    for (String address : config.getAddresses()) {
      String bindAddress = address;
      if (address.contains("|")) {
        List<String> list = Splitter.on('|')
                                    .splitToList(address);
        address = list.get(0);
        bindAddress = list.get(1);
      }
      _targetServers.put(address, new TargetServer(InetAddress.getByName(address), config.getPort(),
          config.getIscsiTargetManager(), InetAddress.getByName(bindAddress), config.getPort()));
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
    for (Future<Void> future : _futures.values()) {
      future.cancel(true);
    }
    _executorService.shutdownNow();
  }

}
