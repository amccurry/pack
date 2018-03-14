package pack.distributed.storage.monitor.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.monitor.PackWriteBlockMonitorEmbedded;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.iscsi.storage.utils.PackUtils;

public class PackDistributedWriteBlockMonitorServer implements MultiWriteBlockMonitor {

  private static final String UTF_8 = "UTF-8";
  private static final Logger LOGGER = LoggerFactory.getLogger(PackDistributedWriteBlockMonitorServer.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    String bindAddress = PackConfig.getWriteBlockMonitorBindAddress();
    int port = PackConfig.getWriteBlockMonitorPort();
    ExecutorService service = Executors.newCachedThreadPool();
    AtomicBoolean _running = new AtomicBoolean(true);
    PackDistributedWriteBlockMonitorServer monitor = new PackDistributedWriteBlockMonitorServer();
    try (ServerSocket serverSocket = new ServerSocket()) {
      InetSocketAddress socketAddress = new InetSocketAddress(bindAddress, port);
      serverSocket.bind(socketAddress);
      LOGGER.info("Listening on {}", socketAddress);
      while (_running.get()) {
        Socket socket = serverSocket.accept();
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        service.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            try (Closer closer = Closer.create()) {
              closer.register(socket);
              DataInputStream inputStream = closer.register(new DataInputStream(socket.getInputStream()));
              DataOutputStream outputStream = closer.register(new DataOutputStream(socket.getOutputStream()));
              while (_running.get()) {
                int read = inputStream.read();
                switch (read) {
                case 0:
                  processAddDirtyBlock(monitor, inputStream, outputStream);
                  break;
                case 1:
                  processResetDirtyBlock(monitor, inputStream, outputStream);
                  break;
                case 2:
                  processWaitIfNeededForSync(monitor, inputStream, outputStream);
                  break;
                case 3:
                  processRegisterVolume(monitor, inputStream, outputStream);
                  break;
                case 4:
                  processRegisterServer(monitor, inputStream, outputStream);
                  break;
                default:
                  break;
                }
                outputStream.flush();
              }
              return null;
            }
          }
        });
      }
    }

  }

  protected static void processWaitIfNeededForSync(PackDistributedWriteBlockMonitorServer monitor,
      DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
    int serverId = inputStream.readInt();
    int volumeId = inputStream.readInt();
    int blockId = inputStream.readInt();
    monitor.waitIfNeededForSync(serverId, volumeId, blockId);
    outputStream.write(0);
  }

  protected static void processResetDirtyBlock(PackDistributedWriteBlockMonitorServer monitor,
      DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
    int serverId = inputStream.readInt();
    int volumeId = inputStream.readInt();
    int blockId = inputStream.readInt();
    long transId = inputStream.readLong();
    monitor.resetDirtyBlock(serverId, volumeId, blockId, transId);
    outputStream.write(0);
  }

  protected static void processAddDirtyBlock(PackDistributedWriteBlockMonitorServer monitor,
      DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
    int volumeId = inputStream.readInt();
    int blockId = inputStream.readInt();
    long transId = inputStream.readLong();
    monitor.addDirtyBlock(volumeId, blockId, transId);
    outputStream.write(0);
  }

  protected static void processRegisterVolume(PackDistributedWriteBlockMonitorServer monitor,
      DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
    String name = readString(inputStream);
    int register = monitor.registerVolume(name);
    outputStream.write(0);
    outputStream.writeInt(register);
  }

  protected static void processRegisterServer(PackDistributedWriteBlockMonitorServer monitor,
      DataInputStream inputStream, DataOutputStream outputStream) throws IOException {
    String name = readString(inputStream);
    int register = monitor.registerServer(name);
    outputStream.write(0);
    outputStream.writeInt(register);
  }

  private static String readString(DataInputStream inputStream) throws IOException {
    int len = inputStream.readInt();
    byte[] buf = new byte[len];
    inputStream.readFully(buf, 0, len);
    return new String(buf, UTF_8);
  }

  private final ConcurrentMap<ServerIdVolumeIdKey, WriteBlockMonitor> _monitorLookup = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Integer> _monitorNameMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Integer> _serverNameMap = new ConcurrentHashMap<>();
  private final AtomicReference<int[]> _serverIds = new AtomicReference<int[]>(new int[] {});
  private final Object _lock = new Object();

  public PackDistributedWriteBlockMonitorServer() {

  }

  @Override
  public void addDirtyBlock(int volumeId, int blockId, long transId) {
    int[] ids = _serverIds.get();
    for (int i = 0; i < ids.length; i++) {
      WriteBlockMonitor writeBlockMonitor = getWriteBlockMonitor(ids[i], volumeId);
      writeBlockMonitor.addDirtyBlock(blockId, transId);
    }
  }

  @Override
  public void resetDirtyBlock(int serverId, int volumeId, int blockId, long transId) {
    WriteBlockMonitor writeBlockMonitor = getWriteBlockMonitor(serverId, volumeId);
    writeBlockMonitor.resetDirtyBlock(blockId, transId);
  }

  @Override
  public void waitIfNeededForSync(int serverId, int volumeId, int blockId) {
    WriteBlockMonitor writeBlockMonitor = getWriteBlockMonitor(serverId, volumeId);
    writeBlockMonitor.waitIfNeededForSync(blockId);
  }

  @Override
  public int registerVolume(String name) throws IOException {
    synchronized (_lock) {
      Integer id = _monitorNameMap.get(name);
      if (id != null) {
        return id;
      }
      int newId = PackUtils.getRandomInt();
      while (_monitorNameMap.containsValue(newId)) {
        newId = PackUtils.getRandomInt();
      }
      _monitorNameMap.put(name, newId);
      int[] ids = _serverIds.get();
      for (int i = 0; i < ids.length; i++) {
        getWriteBlockMonitor(ids[i], newId);
      }
      return newId;
    }
  }

  @Override
  public int registerServer(String server) throws IOException {
    synchronized (_lock) {
      Integer id = _serverNameMap.get(server);
      if (id != null) {
        return id;
      }
      int newId = PackUtils.getRandomInt();
      while (_serverNameMap.containsValue(newId)) {
        newId = PackUtils.getRandomInt();
      }
      _serverNameMap.put(server, newId);
      Collection<Integer> values = _serverNameMap.values();
      int[] ids = new int[values.size()];
      int i = 0;
      for (Integer serverId : values) {
        ids[i++] = serverId;
      }
      _serverIds.set(ids);
      return newId;
    }
  }

  private WriteBlockMonitor getWriteBlockMonitor(int serverId, int volumeId) {
    ServerIdVolumeIdKey key = ServerIdVolumeIdKey.builder()
                                                 .serverId(serverId)
                                                 .volumeId(volumeId)
                                                 .build();
    WriteBlockMonitor writeBlockMonitor = _monitorLookup.get(key);
    if (writeBlockMonitor == null) {
      return newWriteBlockMonitor(key);
    }
    return writeBlockMonitor;
  }

  private WriteBlockMonitor newWriteBlockMonitor(ServerIdVolumeIdKey key) {
    WriteBlockMonitor newWriteBlockMonitor = createWriteBlockMonitor();
    WriteBlockMonitor writeBlockMonitor = _monitorLookup.putIfAbsent(key, newWriteBlockMonitor);
    if (writeBlockMonitor == null) {
      return newWriteBlockMonitor;
    }
    return writeBlockMonitor;
  }

  private WriteBlockMonitor createWriteBlockMonitor() {
    return new PackWriteBlockMonitorEmbedded();
  }

  @Override
  public void close() throws IOException {

  }

}
