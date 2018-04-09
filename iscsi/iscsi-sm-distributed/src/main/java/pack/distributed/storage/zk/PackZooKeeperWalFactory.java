package pack.distributed.storage.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.ImmutableList;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.wal.Block;
import pack.distributed.storage.wal.Blocks;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalReader;
import pack.distributed.storage.wal.PackWalWriter;
import pack.distributed.storage.walcache.WalCacheManager;

public class PackZooKeeperWalFactory extends PackWalFactory {

  // private static final Logger LOGGER =
  // LoggerFactory.getLogger(PackZooKeeperBroadcastFactory.class);
  
  private final ZooKeeperClient _zk;

  public PackZooKeeperWalFactory(ZooKeeperClient zk) {
    _zk = zk;
  }

  @Override
  public PackWalWriter createPackWalWriter(String name, PackMetaData metaData,
      WriteBlockMonitor writeBlockMonitor, BroadcastServerManager serverStatusManager) throws IOException {
    return new PackZooKeeperBroadcastWriter(name, writeBlockMonitor, serverStatusManager, _zk);
  }

  @Override
  public PackWalReader createPackWalReader(String name, PackMetaData metaData,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer) throws IOException {
    return new PackZooKeeperBroadcastReader(name, metaData, walCacheManager, _zk, maxBlockLayer);
  }

  public static String getVolumePath(String volumeName) {
    return "/data/" + volumeName;
  }

  static class PackZooKeeperBroadcastWriter extends PackWalWriter {

    private final ZooKeeperClient _zk;
    private final String _zkPath;

    public PackZooKeeperBroadcastWriter(String volumeName, WriteBlockMonitor writeBlockMonitor,
        BroadcastServerManager serverStatusManager, ZooKeeperClient zk) {
      super(volumeName, writeBlockMonitor, serverStatusManager);
      _zk = zk;
      _zkPath = getVolumePath(volumeName);
      ZkUtils.mkNodesStr(_zk, _zkPath);
    }

    @Override
    protected void writeBlocks(Blocks blocks) throws IOException {
      byte[] bs = Blocks.toBytes(blocks);
      try {
        _zk.create(_zkPath + "/", bs, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        // String path = _zk.create(_zkPath + "/", bs, Ids.OPEN_ACL_UNSAFE,
        // CreateMode.PERSISTENT_SEQUENTIAL);
        // LOGGER.info("write blocks {} {} {}", Md5Utils.md5AsBase64(bs),
        // blocks.hashCode(), path);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void internalFlush() throws IOException {

    }

    @Override
    protected void internalClose() throws IOException {

    }

  }

  static class PackZooKeeperBroadcastReader extends PackWalReader {

    private final ZooKeeperClient _zk;
    private final String _zkPath;
    private final MaxBlockLayer _maxBlockLayer;
    private final AtomicLong _lastOffset = new AtomicLong(-1L);

    public PackZooKeeperBroadcastReader(String volumeName, PackMetaData metaData, WalCacheManager walCacheManager,
        ZooKeeperClient zk, MaxBlockLayer maxBlockLayer) {
      super(volumeName, walCacheManager);
      _zk = zk;
      _zkPath = getVolumePath(volumeName);
      _maxBlockLayer = maxBlockLayer;
      ZkUtils.mkNodesStr(_zk, _zkPath);
    }

    @Override
    public void sync() throws IOException {
      try {
        List<String> allChildren = new ArrayList<>(_zk.getChildren(_zkPath, false));
        if (allChildren.isEmpty()) {
          return;
        }
        Collections.sort(allChildren);
        String last = allChildren.get(allChildren.size() - 1);
        long latestOffset = getOffset(last);
        while (latestOffset > _lastOffset.get()) {
          Thread.sleep(10);
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void writeDataToWal(WalCacheManager walCacheManager) throws IOException {
      Object lock = new Object();

      Watcher watcher = event -> {
        synchronized (lock) {
          lock.notify();
        }
      };

      List<String> prev = ImmutableList.of();
      while (isRunning()) {
        try {
          synchronized (lock) {
            List<String> allChildren = ImmutableList.copyOf(_zk.getChildren(_zkPath, watcher));
            cleanupOldData(allChildren);
            List<String> delta = new ArrayList<>(allChildren);
            delta.removeAll(prev);
            Collections.sort(delta);
            for (String s : delta) {
              Stat stat = _zk.exists(_zkPath + "/" + s, false);
              if (stat != null) {
                byte[] bs = _zk.getData(_zkPath + "/" + s, false, stat);
                Blocks blocks = Blocks.toBlocks(bs);
                // LOGGER.info("read blocks {} {} {}", Md5Utils.md5AsBase64(bs),
                // blocks.hashCode(), _zkPath + "/" + s);
                List<Block> blocksList = blocks.getBlocks();
                long offset = getOffset(s);
                _lastOffset.set(offset);
                for (Block block : blocksList) {
                  int blockId = block.getBlockId();
                  long transId = block.getTransId();
                  byte[] data = block.getData();
                  // LOGGER.info("wal write {} {} {}", transId, offset,
                  // blockId);
                  walCacheManager.write(transId, offset, blockId, data);
                }
              }
            }
            prev = allChildren;
            lock.wait();
          }
        } catch (KeeperException | InterruptedException e) {
          throw new IOException(e);
        }
      }
    }

    private long getOffset(String s) {
      return Long.parseLong(s);
    }

    private void cleanupOldData(List<String> allChildren) throws InterruptedException, KeeperException {
      long maxLayer = _maxBlockLayer.getMaxLayer();
      for (String s : allChildren) {
        long offset = getOffset(s);
        if (offset < maxLayer) {
          try {
            _zk.delete(_zkPath + "/" + s, -1);
          } catch (KeeperException e) {
            if (e.code() != Code.NONODE) {
              throw e;
            }
          }
        }
      }
    }
  }

}
