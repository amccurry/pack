package pack.distributed.storage.monitor.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.monitor.PackWriteBlockMonitorEmbedded;
import pack.distributed.storage.monitor.WriteBlockMonitor;

public class PackWriteBlockMonitorServerRaw implements WriteBlockMonitorRaw {

  private static final String UTF_8 = "UTF-8";
  private static final Logger LOGGER = LoggerFactory.getLogger(PackWriteBlockMonitorServerRaw.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    String bindAddress = PackConfig.getWriteBlockMonitorBindAddress();
    int port = PackConfig.getWriteBlockMonitorPort();
    ExecutorService service = Executors.newCachedThreadPool();
    AtomicBoolean _running = new AtomicBoolean(true);
    PackWriteBlockMonitorServerRaw monitor = new PackWriteBlockMonitorServerRaw();
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
                  processRegister(monitor, inputStream, outputStream);
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

  protected static void processWaitIfNeededForSync(PackWriteBlockMonitorServerRaw monitor, DataInputStream inputStream,
      DataOutputStream outputStream) throws IOException {
    int id = inputStream.readInt();
    int blockId = inputStream.readInt();
    monitor.waitIfNeededForSync(id, blockId);
    outputStream.write(0);
  }

  protected static void processResetDirtyBlock(PackWriteBlockMonitorServerRaw monitor, DataInputStream inputStream,
      DataOutputStream outputStream) throws IOException {
    int id = inputStream.readInt();
    int blockId = inputStream.readInt();
    long transId = inputStream.readLong();
    monitor.resetDirtyBlock(id, blockId, transId);
    outputStream.write(0);
  }

  protected static void processAddDirtyBlock(PackWriteBlockMonitorServerRaw monitor, DataInputStream inputStream,
      DataOutputStream outputStream) throws IOException {
    int id = inputStream.readInt();
    int blockId = inputStream.readInt();
    long transId = inputStream.readLong();
    monitor.addDirtyBlock(id, blockId, transId);
    outputStream.write(0);
  }

  protected static void processRegister(PackWriteBlockMonitorServerRaw monitor, DataInputStream inputStream,
      DataOutputStream outputStream) throws IOException {
    String name = readString(inputStream);
    int register = monitor.register(name);
    outputStream.write(0);
    outputStream.writeInt(register);
  }

  private static String readString(DataInputStream inputStream) throws IOException {
    int len = inputStream.readInt();
    byte[] buf = new byte[len];
    inputStream.readFully(buf, 0, len);
    return new String(buf, UTF_8);
  }

  private final WriteBlockMonitor[] _monitorLookup = new WriteBlockMonitor[10000];
  private final ConcurrentMap<String, Integer> _monitorNameMap = new ConcurrentHashMap<>();

  @Override
  public void addDirtyBlock(int id, int blockId, long transId) {
    getWriteBlockMonitor(id).addDirtyBlock(blockId, transId);
  }

  @Override
  public void resetDirtyBlock(int id, int blockId, long transId) {
    getWriteBlockMonitor(id).resetDirtyBlock(blockId, transId);
  }

  @Override
  public void waitIfNeededForSync(int id, int blockId) {
    getWriteBlockMonitor(id).waitIfNeededForSync(blockId);
  }

  @Override
  public synchronized int register(String name) throws IOException {
    Integer id = _monitorNameMap.get(name);
    if (id != null) {
      return id;
    }
    int size = _monitorNameMap.size();
    _monitorNameMap.put(name, size);
    getWriteBlockMonitor(size);
    return size;
  }

  private WriteBlockMonitor getWriteBlockMonitor(int id) {
    WriteBlockMonitor writeBlockMonitor = _monitorLookup[id];
    if (writeBlockMonitor == null) {
      return newWriteBlockMonitor(id);
    }
    return writeBlockMonitor;
  }

  private WriteBlockMonitor newWriteBlockMonitor(int id) {
    WriteBlockMonitor newWriteBlockMonitor = createWriteBlockMonitor();
    WriteBlockMonitor writeBlockMonitor = _monitorLookup[id];
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
