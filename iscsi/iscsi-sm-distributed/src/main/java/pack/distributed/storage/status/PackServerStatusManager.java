package pack.distributed.storage.status;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.utils.PackUtils;

public class PackServerStatusManager implements Closeable, ServerStatusManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackServerStatusManager.class);

  private static final String SERVERS = "/servers";
  private final AtomicReference<Set<String>> _servers = new AtomicReference<Set<String>>();
  private final Map<String, WriteBlockMonitor> _monitorMap = new ConcurrentHashMap<>();
  private final DatagramSocket _socket;
  private final ExecutorService _service;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final ZooKeeperClient _zk;
  private final int _writeBlockMonitorPort;
  private final String _writeBlockMonitorAddress;
  private final Object _serverLock = new Object();

  public PackServerStatusManager(ZooKeeperClient zk, String writeBlockMonitorBindAddress, int writeBlockMonitorPort,
      String writeBlockMonitorAddress) throws IOException {
    _writeBlockMonitorPort = writeBlockMonitorPort;
    _zk = zk;
    _writeBlockMonitorAddress = writeBlockMonitorAddress;
    ZkUtils.mkNodesStr(zk, SERVERS);
    try {
      zk.create(SERVERS + "/" + writeBlockMonitorAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    _socket = new DatagramSocket(writeBlockMonitorPort, InetAddress.getByName(writeBlockMonitorBindAddress));
    _service = Executors.newCachedThreadPool();
    startListener();
    startServerWatcher();
  }

  private void startListener() {
    _service.submit(() -> {
      while (_running.get()) {
        try {
          listen();
        } catch (IOException e) {
          LOGGER.error("Unknown error trying to recv data packet", e);
        }
      }
    });
  }

  private void listen() throws IOException {
    byte[] buf = new byte[1000];
    DatagramPacket p = new DatagramPacket(buf, buf.length);
    while (_running.get()) {
      _socket.receive(p);
      byte[] data = p.getData();
      int off = p.getOffset();
      int len = p.getLength();
      int blockId = PackUtils.getInt(data, off + 0);
      long transId = PackUtils.getLong(data, off + 4);
      String name = new String(data, off + 12, len - 12);
      WriteBlockMonitor writeBlockMonitor = _monitorMap.get(name);
      writeBlockMonitor.addDirtyBlock(blockId, transId);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see pack.distributed.storage.status.SSM#isLeader(java.lang.String)
   */
  @Override
  public boolean isLeader(String name) {
    synchronized (_serverLock) {
      Set<String> servers = _servers.get();
      if (servers == null || servers.isEmpty()) {
        return false;
      }
      List<String> list = new ArrayList<>(servers);
      Collections.sort(list);
      int index = Math.abs(name.hashCode()) % list.size();
      String server = list.get(index);
      return server.equals(_writeBlockMonitorAddress);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see pack.distributed.storage.status.SSM#register(java.lang.String,
   * pack.distributed.storage.monitor.WriteBlockMonitor)
   */
  @Override
  public void register(String name, WriteBlockMonitor monitor) {
    _monitorMap.put(name, monitor);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * pack.distributed.storage.status.SSM#broadcastToAllServers(java.lang.String,
   * int, long)
   */
  @Override
  public void broadcastToAllServers(String name, int blockId, long transId) {
    Set<String> servers = _servers.get();
    LOGGER.debug("broadcastToAllServers {} {} {} {}", name, blockId, transId, servers);
    if (servers == null || servers.isEmpty()) {
      return;
    } else if (servers.size() == 1 && servers.contains(_writeBlockMonitorAddress)) {
      return;
    }
    byte[] bs = name.getBytes();
    byte[] buf = new byte[bs.length + 4 + 8];
    PackUtils.putInt(buf, 0, blockId);
    PackUtils.putLong(buf, 4, transId);
    System.arraycopy(bs, 0, buf, 12, bs.length);
    try {
      DatagramPacket datagramPacket = new DatagramPacket(buf, buf.length);
      datagramPacket.setPort(_writeBlockMonitorPort);
      for (String server : servers) {

        datagramPacket.setAddress(InetAddress.getByName(server));
        _socket.send(datagramPacket);
      }
    } catch (IOException e) {
      LOGGER.error("Unknown error trying to send data packet", e);
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    PackUtils.close(LOGGER, _service);
    PackUtils.close(LOGGER, _zk, _socket);
  }

  private void startServerWatcher() {
    _service.submit(() -> {
      while (_running.get()) {
        try {
          serverWatch();
        } catch (KeeperException | InterruptedException e) {
          LOGGER.error("Unknown error during server watch", e);
        }
      }
    });
  }

  private void serverWatch() throws KeeperException, InterruptedException {
    Watcher watch = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        synchronized (_serverLock) {
          _serverLock.notify();
        }
      }
    };
    while (_running.get()) {
      synchronized (_serverLock) {
        _servers.set(ImmutableSet.copyOf(_zk.getChildren(SERVERS, watch)));
        _serverLock.wait();
      }
    }
  }
}
