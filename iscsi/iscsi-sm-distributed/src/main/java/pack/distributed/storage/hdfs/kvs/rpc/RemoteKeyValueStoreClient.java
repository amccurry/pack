package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.utils.PackUtils;

public class RemoteKeyValueStoreClient implements RemoteKeyValueStore, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteKeyValueStoreClient.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation ugi = PackConfig.getUgi();
    RPC.setProtocolEngine(configuration, RemoteKeyValueStore.class, WritableRpcEngine.class);
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      String zooKeeperConnection = PackConfig.getHdfsWalZooKeeperConnection();
      int sessionTimeout = PackConfig.getZooKeeperSessionTimeout();
      ZooKeeperClient zk = ZkUtils.newZooKeeper(zooKeeperConnection, sessionTimeout);
      try (RemoteKeyValueStoreClient rkvs = RemoteKeyValueStoreClient.create(configuration, zk)) {
        Key key = Key.toKey(BytesRef.value(123L));
        {
          GetResult result = rkvs.get("test", key);
          if (result.isFound()) {
            System.out.println(result.getValue()
                                     .utf8ToString());
          } else {
            System.err.println("NOT FOUND");
          }
        }

        TransId transId = rkvs.put("test", key,
            Key.toKey(new BytesRef("test hi there! " + System.currentTimeMillis())));

        rkvs.sync("test", transId);

        {
          GetResult result = rkvs.get("test", key);
          System.out.println(result.getValue()
                                   .utf8ToString());
        }
      }
      return null;
    });
  }

  public static RemoteKeyValueStoreClient create(Configuration configuration, ZooKeeperClient zk) throws IOException {
    return new RemoteKeyValueStoreClient(configuration, zk);
  }

  private final Configuration _configuration;
  private final ZooKeeperClient _zk;
  private final Thread _watchThread;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final Map<String, String> _storeToServerMap = new ConcurrentHashMap<>();
  private final Map<String, RemoteKeyValueStore> _serverToClientMap = new ConcurrentHashMap<>();

  private RemoteKeyValueStoreClient(Configuration configuration, ZooKeeperClient zk) throws IOException {
    _configuration = configuration;
    _zk = zk;
    ZkUtils.mkNodes(_zk, RemoteKeyValueStoreServer.SERVERS);
    ZkUtils.mkNodes(_zk, RemoteKeyValueStoreServer.STORES);
    _watchThread = new Thread(() -> {
      while (isRunning()) {
        try {
          watchForKvs();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    });
    _watchThread.setDaemon(true);
    _watchThread.start();
  }

  private void watchForKvs() throws KeeperException, InterruptedException {
    Object lock = new Object();
    Watcher watch = event -> {
      synchronized (lock) {
        lock.notify();
      }
    };
    _storeToServerMap.clear();
    List<String> prev = new ArrayList<>();
    while (isRunning()) {
      synchronized (lock) {
        List<String> kvsList = new ArrayList<>(_zk.getChildren(RemoteKeyValueStoreServer.STORES, watch));
        updateKvs(kvsList);
        prev.removeAll(kvsList);
        for (String store : prev) {
          _storeToServerMap.remove(store);
        }
        prev = kvsList;
        lock.wait();
      }
    }
  }

  private void updateKvs(List<String> kvsList) throws KeeperException, InterruptedException {
    for (String store : kvsList) {
      Stat stat = _zk.exists(RemoteKeyValueStoreServer.STORES, false);
      if (stat != null) {
        byte[] data = _zk.getData(RemoteKeyValueStoreServer.STORES + "/" + store, false, stat);
        _storeToServerMap.put(store, new String(data));
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _watchThread.interrupt();
  }

  @Override
  public StoreList storeList() throws IOException {
    Builder<Pair<String, String>> builder = ImmutableList.builder();
    for (Entry<String, String> e : _storeToServerMap.entrySet()) {
      builder.add(Pair.create(e.getKey(), e.getValue()));
    }
    return StoreList.builder()
                    .stores(builder.build())
                    .build();
  }

  @Override
  public ScanResult scan(String store, Key key) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.scan(store, key);
  }

  @Override
  public Key lastKey(String store) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.lastKey(store);
  }

  @Override
  public GetResult get(String store, Key key) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.get(store, key);
  }

  @Override
  public TransId put(String store, Key key, Key value) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.put(store, key, value);
  }

  @Override
  public TransId delete(String store, Key key) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.delete(store, key);
  }

  @Override
  public TransId deleteRange(String store, Key fromInclusive, Key toExclusive) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    return rkvs.deleteRange(store, fromInclusive, toExclusive);
  }

  @Override
  public void sync(String store, TransId transId) throws IOException {
    RemoteKeyValueStore rkvs = getRemoteKeyValueStore(store);
    rkvs.sync(store, transId);
  }

  private RemoteKeyValueStore getRemoteKeyValueStore(String store) throws IOException {
    String server = _storeToServerMap.get(store);
    if (server == null) {
      server = assignLiveServer();
    }
    RemoteKeyValueStore remoteKeyValueStore = _serverToClientMap.get(server);
    if (remoteKeyValueStore == null) {
      try {
        return newRemoteKeyValueStore(server);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    return remoteKeyValueStore;
  }

  private String assignLiveServer() throws IOException {
    try {
      List<String> list = new ArrayList<>(_zk.getChildren(RemoteKeyValueStoreServer.SERVERS, false));
      if (list.isEmpty()) {
        throw new IOException("No live servers");
      }
      int index = PackUtils.getRandom()
                           .nextInt(list.size());
      return list.get(index);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private synchronized RemoteKeyValueStore newRemoteKeyValueStore(String server)
      throws IOException, InterruptedException {
    int indexOf = server.indexOf(":");
    String address = server.substring(0, indexOf);
    String portStr = server.substring(indexOf + 1);
    RemoteKeyValueStore rkvs = _serverToClientMap.get(server);
    if (rkvs == null) {
      _serverToClientMap.put(server, rkvs = createRemoteKeyValueStoreRpc(address, Integer.parseInt(portStr)));
    }
    return rkvs;
  }

  private RemoteKeyValueStore createRemoteKeyValueStoreRpc(String address, int port)
      throws IOException, InterruptedException {
    InetSocketAddress dataNodeAddress = new InetSocketAddress(address, port);
    return RPC.getProtocolProxy(RemoteKeyValueStore.class, RPC.getProtocolVersion(RemoteKeyValueStore.class),
        dataNodeAddress, UserGroupInformation.getCurrentUser(), _configuration,
        NetUtils.getDefaultSocketFactory(_configuration))
              .getProxy();
  }

  private boolean isRunning() {
    return _running.get();
  }
}
