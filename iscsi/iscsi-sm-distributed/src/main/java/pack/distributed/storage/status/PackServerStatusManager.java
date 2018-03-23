package pack.distributed.storage.status;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

public class PackServerStatusManager implements ServerStatusManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackServerStatusManager.class);

  private static final String STATUS_SERVERS = "/status-servers";
  private final AtomicReference<Set<String>> _servers = new AtomicReference<Set<String>>();
  private final Map<String, WriteBlockMonitor> _monitorMap = new ConcurrentHashMap<>();
  private final ExecutorService _service;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final ZooKeeperClient _zk;
  private final int _writeBlockMonitorPort;
  private final String _writeBlockMonitorAddress;
  private final Object _serverLock = new Object();
  private final Map<String, BlockingQueue<BlockUpdateInfoBatch>> _sendingQueue = new ConcurrentHashMap<>();
  private final int _queueDepth = 64;
  private final ServerSocket _serverSocket;

  public PackServerStatusManager(ZooKeeperClient zk, String writeBlockMonitorBindAddress, int writeBlockMonitorPort,
      String writeBlockMonitorAddress) throws IOException {
    _writeBlockMonitorPort = writeBlockMonitorPort;
    _zk = zk;
    _writeBlockMonitorAddress = writeBlockMonitorAddress;
    ZkUtils.mkNodesStr(zk, STATUS_SERVERS);
    try {
      zk.create(STATUS_SERVERS + "/" + writeBlockMonitorAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
    _serverSocket = new ServerSocket();
    _serverSocket.bind(new InetSocketAddress(_writeBlockMonitorAddress, _writeBlockMonitorPort));
    _service = Executors.newFixedThreadPool(100);
    startReceiver();
    startServerWatcher();
  }

  private BlockingQueue<BlockUpdateInfoBatch> startSender(String server, BlockingQueue<BlockUpdateInfoBatch> queue) {
    _service.submit(() -> {
      while (_running.get()) {
        try {
          send(server, queue);
        } catch (IOException | InterruptedException e) {
          LOGGER.error("Unknown error", e);
        }
      }
    });
    return queue;
  }

  private void startReceiver() {
    _service.submit(() -> {
      while (_running.get()) {
        try {
          Socket socket = _serverSocket.accept();
          _service.submit(new Runnable() {
            @Override
            public void run() {
              try {
                receive(socket);
              } catch (IOException e) {
                LOGGER.error("Unknown error with socket " + socket, e);
              } finally {
                PackUtils.close(LOGGER, socket);
              }
            }
          });
        } catch (IOException e) {
          LOGGER.error("Unknown error", e);
        }
      }
    });
  }

  private void send(String server, BlockingQueue<BlockUpdateInfoBatch> queue) throws IOException, InterruptedException {
    try (Socket socket = new Socket(server, _writeBlockMonitorPort)) {
      socket.setTcpNoDelay(true);
      socket.setKeepAlive(true);
      try (DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream())) {
        List<BlockUpdateInfoBatch> drain = new ArrayList<>();
        while (_running.get()) {
          {
            BlockUpdateInfoBatch ubib = queue.take();
            queue.drainTo(drain);
            write(outputStream, ubib);
          }
          for (BlockUpdateInfoBatch ubib : drain) {
            write(outputStream, ubib);
          }
          drain.clear();
        }
      }
    }
  }

  private void write(DataOutput output, BlockUpdateInfoBatch updateBlockIdBatch) throws IOException {
    write(output, updateBlockIdBatch.getVolume());
    List<BlockUpdateInfo> batch = updateBlockIdBatch.getBatch();
    int size = batch.size();
    output.writeInt(size);
    for (int i = 0; i < size; i++) {
      write(output, batch.get(i));
    }
  }

  private void write(DataOutput output, BlockUpdateInfo updateBlockId) throws IOException {
    output.writeInt(updateBlockId.getBlockId());
    output.writeLong(updateBlockId.getTransId());
  }

  private void write(DataOutput output, String s) throws IOException {
    byte[] bs = s.getBytes();
    output.writeInt(bs.length);
    output.write(bs);
  }

  private BlockUpdateInfoBatch readUpdateBlockIdBatch(DataInput input) throws IOException {
    String volume = readString(input);
    List<BlockUpdateInfo> batch = new ArrayList<>();
    int count = input.readInt();
    for (int i = 0; i < count; i++) {
      batch.add(readUpdateBlockId(input));
    }
    return BlockUpdateInfoBatch.builder()
                               .batch(batch)
                               .volume(volume)
                               .build();
  }

  private BlockUpdateInfo readUpdateBlockId(DataInput input) throws IOException {
    return BlockUpdateInfo.builder()
                          .blockId(input.readInt())
                          .transId(input.readLong())
                          .build();
  }

  private String readString(DataInput input) throws IOException {
    int len = input.readInt();
    byte[] buf = new byte[len];
    input.readFully(buf);
    return new String(buf);
  }

  private void receive(Socket socket) throws IOException {
    try (DataInputStream inputStream = new DataInputStream(socket.getInputStream())) {
      while (_running.get()) {
        BlockUpdateInfoBatch ubib = readUpdateBlockIdBatch(inputStream);
        WriteBlockMonitor writeBlockMonitor = _monitorMap.get(ubib.getVolume());
        List<BlockUpdateInfo> list = ubib.getBatch();
        for (BlockUpdateInfo blockUpdateInfo : list) {
          writeBlockMonitor.addDirtyBlock(blockUpdateInfo.getBlockId(), blockUpdateInfo.getTransId());
        }
      }
    }
  }

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

  @Override
  public void register(String name, WriteBlockMonitor monitor) {
    _monitorMap.put(name, monitor);
  }

  @Override
  public void broadcastToAllServers(BlockUpdateInfoBatch updateBlockIdBatch) {
    Set<String> servers = _servers.get();
    if (servers == null || servers.isEmpty()) {
      return;
    } else if (servers.size() == 1 && servers.contains(_writeBlockMonitorAddress)) {
      return;
    }
    try {
      for (String server : servers) {
        BlockingQueue<BlockUpdateInfoBatch> queue = getSendingQueue(server);
        queue.put(updateBlockIdBatch);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Unknown error trying to send block update info", e);
    }
  }

  private BlockingQueue<BlockUpdateInfoBatch> getSendingQueue(String server) {
    BlockingQueue<BlockUpdateInfoBatch> queue = _sendingQueue.get(server);
    if (queue == null) {
      return newSendingQueue(server);
    }
    return queue;
  }

  private BlockingQueue<BlockUpdateInfoBatch> newSendingQueue(String server) {
    BlockingQueue<BlockUpdateInfoBatch> newQueue = new ArrayBlockingQueue<>(_queueDepth);
    BlockingQueue<BlockUpdateInfoBatch> current = _sendingQueue.putIfAbsent(server, newQueue);
    if (current == null) {
      return startSender(server, newQueue);
    }
    return current;
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    PackUtils.close(LOGGER, _service);
    PackUtils.close(LOGGER, _zk, _serverSocket);
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
        _servers.set(ImmutableSet.copyOf(_zk.getChildren(STATUS_SERVERS, watch)));
        _serverLock.wait();
      }
    }
  }

}
