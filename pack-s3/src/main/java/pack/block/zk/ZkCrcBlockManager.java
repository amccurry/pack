package pack.block.zk;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class ZkCrcBlockManager implements CrcBlockManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkCrcBlockManager.class);

  private final CuratorFramework _client;
  private final String _volume;

  public ZkCrcBlockManager(ZkCrcBlockManagerConfig config) {
    _volume = config.getVolume();
    RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
    _client = CuratorFrameworkFactory.newClient(config.getZk(), retryPolicy);
    _client.getUnhandledErrorListenable()
           .addListener((message, e) -> {
             LOGGER.error("Unknown error " + message, e);
           });
    _client.getConnectionStateListenable()
           .addListener((c, newState) -> {
             LOGGER.info("Connection state {}", newState);
           });
    _client.start();
  }

  @Override
  public long getBlockCrc(long blockId) throws Exception {
    String path = getPath(blockId);
    Stat stat = _client.checkExists()
                       .forPath(path);
    if (stat == null) {
      return CRC64.DEFAULT_VALUE;
    }
    byte[] bs = _client.getData()
                       .forPath(path);
    if (bs == null) {
      return CRC64.DEFAULT_VALUE;
    }
    return Long.parseLong(new String(bs));
  }

  @Override
  public void putBlockCrc(long blockId, long crc) throws Exception {
    String path = getPath(blockId);
    Stat stat = _client.checkExists()
                       .forPath(path);
    if (stat == null) {
      _client.create()
             .creatingParentsIfNeeded()
             .forPath(path, toBytes(crc));
    } else {
      _client.setData()
             .forPath(path, toBytes(crc));
    }
  }

  private String getPath(long blockId) {
    return "/" + _volume + "/" + blockId;
  }

  private byte[] toBytes(long crc) {
    String str = Long.toString(crc);
    return str.getBytes();
  }

  @Override
  public void sync() {

  }

  @Override
  public void close() {
    _client.close();
  }

}
