package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.ipc.WritableRpcEngine;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.hdfs.kvs.HdfsKeyValueStore;
import pack.distributed.storage.hdfs.kvs.KeyValueStore;
import pack.distributed.storage.hdfs.kvs.KeyValueStoreTransId;
import pack.distributed.storage.zk.ZkUtils;
import pack.distributed.storage.zk.ZooKeeperClient;
import pack.iscsi.storage.utils.PackUtils;

public class RemoteKeyValueStoreServer implements RemoteKeyValueStore, Closeable {

  public static final String SERVERS = "/servers";
  public static final String STORES = "/stores";

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteKeyValueStoreServer.class);

  private static final String KVS_TIMER = "kvs-timer";
  private static boolean _embeddedLookup = true;

  public static void main(String[] args) throws IOException, InterruptedException {
    Path rootKvs = PackConfig.getHdfsWalDir();
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation.setConfiguration(configuration);
    UserGroupInformation ugi = PackConfig.getUgi();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      String bindAddress = PackConfig.getHdfsWalBindAddress();
      int port = PackConfig.getHdfsWalPort();

      String zooKeeperConnection = PackConfig.getHdfsWalZooKeeperConnection();
      int sessionTimeout = PackConfig.getZooKeeperSessionTimeout();

      ZooKeeperClient zooKeeper = ZkUtils.newZooKeeper(zooKeeperConnection, sessionTimeout);

      RPC.setProtocolEngine(configuration, RemoteKeyValueStore.class, WritableRpcEngine.class);

      try (RemoteKeyValueStoreServer remoteKeyValueStoreServer = RemoteKeyValueStoreServer.createInstance(bindAddress,
          port, configuration, zooKeeper, rootKvs, ugi)) {
        RemoteKeyValueStoreServer instance = lookupInstance(bindAddress, port);
        instance.start();
        instance.join();
      }
      return null;
    });
  }

  public static boolean isEmbeddedLookup() {
    return _embeddedLookup;
  }

  public static void setEmbeddedLookup(boolean embeddedLookup) {
    _embeddedLookup = embeddedLookup;
  }

  private static final Map<String, RemoteKeyValueStoreServer> _instances = new ConcurrentHashMap<>();

  public static synchronized RemoteKeyValueStoreServer lookupInstance(String address, int port) throws IOException {
    if (!_embeddedLookup) {
      return null;
    }
    return _instances.get(getKey(address, port));
  }

  public static synchronized RemoteKeyValueStoreServer createInstance(String bindAddress, int port,
      Configuration configuration, ZooKeeperClient zooKeeper, Path rootKvs, UserGroupInformation ugi)
      throws IOException {
    String key = getKey(bindAddress, port);
    RemoteKeyValueStoreServer server = _instances.get(key);
    if (server == null) {
      server = new RemoteKeyValueStoreServer(configuration, bindAddress, port, zooKeeper, rootKvs, ugi);
      _instances.put(key, server);
    }
    return server;
  }

  private static String getKey(String address, int port) throws IOException {
    if (address.equals("0.0.0.0")) {
      address = InetAddress.getLocalHost()
                           .getHostName();
    }
    return address + ":" + port;
  }

  private final Configuration _configuration;
  private final String _bindAddress;
  private final int _port;
  private final Map<String, KeyValueStore> _stores = new ConcurrentHashMap<>();
  private final ZooKeeperClient _zooKeeper;
  private final Timer _hdfsKeyValueTimer;
  private final Path _rootKvs;
  private final long _maxResponseSize = 1024 * 1024;
  private final Server _server;
  private final String _serverAddress;
  private final UserGroupInformation _ugi;

  RemoteKeyValueStoreServer(Configuration configuration, String bindAddress, int port, ZooKeeperClient zooKeeper,
      Path rootKvs, UserGroupInformation ugi) throws IOException {
    _ugi = ugi;
    _rootKvs = rootKvs;
    _zooKeeper = zooKeeper;
    ZkUtils.mkNodes(zooKeeper, SERVERS);
    ZkUtils.mkNodes(zooKeeper, STORES);
    _configuration = configuration;
    _bindAddress = bindAddress;
    _port = port;
    _serverAddress = getKey(bindAddress, port);
    _hdfsKeyValueTimer = new Timer(KVS_TIMER, true);
    _server = new RPC.Builder(_configuration).setBindAddress(_bindAddress)
                                             .setPort(_port)
                                             .setInstance(this)
                                             .setProtocol(RemoteKeyValueStore.class)
                                             .build();
    ServiceAuthorizationManager serviceAuthorizationManager = _server.getServiceAuthorizationManager();
    serviceAuthorizationManager.refresh(_configuration, new RemoteKeyValueStorePolicyProvider());
  }

  public String getServerAddress() {
    return _serverAddress;
  }

  public void join() throws InterruptedException {
    _server.join();
  }

  public void start() throws IOException {
    _server.start();
    try {
      _zooKeeper.create(SERVERS + "/" + _serverAddress, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public StoreList storeList() throws IOException {
    try {
      List<String> list = _zooKeeper.getChildren(STORES, false);
      Builder<Pair<String, String>> builder = ImmutableList.builder();
      for (String s : list) {
        Stat stat = _zooKeeper.exists(getStoresZkPath(s), false);
        if (stat != null) {
          byte[] bs = _zooKeeper.getData(getStoresZkPath(s), false, stat);
          builder.add(Pair.create(s, new String(bs)));
        }
      }
      return StoreList.builder()
                      .stores(builder.build())
                      .build();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private String getStoresZkPath(String s) {
    return STORES + "/" + s;
  }

  @Override
  public ScanResult scan(String store, BytesReference key) throws IOException {
    KeyValueStore kvs = getStore(store);
    Builder<Pair<BytesRef, BytesRef>> builder = ImmutableList.builder();
    long responseSize = 0;
    for (Entry<BytesRef, BytesRef> e : kvs.scan(key.getBytesRef())) {
      BytesRef value = e.getValue();
      responseSize += value.length;
      builder.add(Pair.create(e.getKey(), value));
      if (responseSize >= _maxResponseSize) {
        break;
      }
    }
    return ScanResult.builder()
                     .result(builder.build())
                     .build();
  }

  @Override
  public BytesReference lastKey(String store) throws IOException {
    KeyValueStore kvs = getStore(store);
    return BytesReference.toBytesReference(kvs.lastKey());
  }

  @Override
  public GetResult get(String store, BytesReference key) throws IOException {
    KeyValueStore kvs = getStore(store);
    BytesRef value = new BytesRef();
    boolean found = kvs.get(key.getBytesRef(), value);
    return GetResult.builder()
                    .found(found)
                    .value(value)
                    .build();
  }

  @Override
  public TransId putIncrement(String store, BytesReference initialKey, BytesReference value) throws IOException {
    KeyValueStore kvs = getStore(store);
    KeyValueStoreTransId transId = kvs.putIncrement(initialKey.getBytesRef(), value.getBytesRef());
    return TransId.toTransId(transId);
  }

  @Override
  public TransId put(String store, BytesReference key, BytesReference value) throws IOException {
    KeyValueStore kvs = getStore(store);
    KeyValueStoreTransId transId = kvs.put(key.getBytesRef(), value.getBytesRef());
    return TransId.toTransId(transId);
  }

  @Override
  public TransId delete(String store, BytesReference key) throws IOException {
    KeyValueStore kvs = getStore(store);
    KeyValueStoreTransId transId = kvs.delete(key.getBytesRef());
    return TransId.toTransId(transId);
  }

  @Override
  public TransId deleteRange(String store, BytesReference fromInclusive, BytesReference toExclusive)
      throws IOException {
    KeyValueStore kvs = getStore(store);
    KeyValueStoreTransId transId = kvs.deleteRange(fromInclusive.getBytesRef(), toExclusive.getBytesRef());
    return TransId.toTransId(transId);
  }

  @Override
  public void sync(String store, TransId transId) throws IOException {
    KeyValueStore kvs = getStore(store);
    kvs.sync(TransId.toKeyValueStoreTransId(transId));
  }

  private KeyValueStore getStore(String store) throws IOException {
    KeyValueStore keyValueStore = _stores.get(store);
    if (keyValueStore == null) {
      return newStore(store);
    }
    return keyValueStore;
  }

  private synchronized KeyValueStore newStore(String store) throws IOException {
    KeyValueStore keyValueStore = _stores.get(store);
    if (keyValueStore == null) {
      _stores.put(store, keyValueStore = createStore(store));
    }
    return keyValueStore;
  }

  private KeyValueStore createStore(String store) throws IOException {
    try {
      String path = getStoresZkPath(store);
      Stat stat = _zooKeeper.exists(path, false);
      if (stat == null) {
        try {
          _zooKeeper.create(path, _serverAddress.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
          if (e.code() == Code.NODEEXISTS) {
            throw new NotServingKvs();
          }
          LOGGER.error("Error trying to assign kvs " + store, e);
          throw new IOException(e.getMessage());
        }
        return UgiKeyValueStore.wrap(_ugi,
            _ugi.doAs((PrivilegedExceptionAction<KeyValueStore>) () -> new HdfsKeyValueStore(false, _hdfsKeyValueTimer,
                _configuration, new Path(_rootKvs, store))));
      } else {
        throw new NotServingKvs();
      }
    } catch (KeeperException | InterruptedException e) {
      LOGGER.error("Error trying to assign kvs " + store, e);
      throw new IOException(e.getMessage());
    }
  }

  public static class RemoteKeyValueStorePolicyProvider extends PolicyProvider {
    @Override
    public Service[] getServices() {
      return new Service[] { new RemoteKeyValueStoreService() };
    }
  }

  public static class RemoteKeyValueStoreService extends Service {

    private static final String SECURITY_REMOTE_KAY_VALUE_STORE_PROTOCOL_ACL = "security.remote.kay.value.store.protocol.acl";

    public RemoteKeyValueStoreService() {
      super(SECURITY_REMOTE_KAY_VALUE_STORE_PROTOCOL_ACL, RemoteKeyValueStore.class);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      _zooKeeper.delete(SERVERS + "/" + _serverAddress, -1);
    } catch (InterruptedException | KeeperException e) {
      LOGGER.info("Error while removing entry in zk", e);
    }

    _instances.remove(_serverAddress);
    _hdfsKeyValueTimer.cancel();
    _hdfsKeyValueTimer.purge();
    _server.stop();

    Collection<KeyValueStore> values = _stores.values();
    for (KeyValueStore keyValueStore : values) {
      PackUtils.close(LOGGER, keyValueStore);
    }
  }

}
