package pack.block.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import pack.block.CrcBlockManager;
import pack.block.util.CRC64;

public class ZkCrcBlockManager implements CrcBlockManager {

  private final CuratorFramework _client;
  private final String _volume;

  public ZkCrcBlockManager(ZkCrcBlockManagerConfig config) {
    _volume = config.getVolume();
    _client = config.getClient();
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

  }

}
