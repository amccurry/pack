package pack.distributed.storage.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import lombok.val;
import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.hdfs.kvs.HdfsKeyValueStore;
import pack.distributed.storage.hdfs.kvs.TransId;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.wal.Block;
import pack.distributed.storage.wal.Blocks;
import pack.distributed.storage.wal.PackWalFactory;
import pack.distributed.storage.wal.PackWalReader;
import pack.distributed.storage.wal.PackWalWriter;
import pack.distributed.storage.walcache.WalCacheManager;
import pack.iscsi.storage.utils.PackUtils;

public class PackHdfsWalFactory extends PackWalFactory {

  private static final String HDFS_WAL_TIMER = "hdfs-wal-timer";

  private final Timer _hdfsKeyValueTimer;
  private final Configuration _configuration;
  private Path _root;
  private long _maxAmountAllowedPerFile;
  private long _maxTimeOpenForWriting;

  public PackHdfsWalFactory(Configuration configuration, Path rootWal) {
    _configuration = configuration;
    _root = rootWal;
    _maxAmountAllowedPerFile = PackConfig.getHdfsWalMaxAmountAllowedPerFile(
        HdfsKeyValueStore.DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE);
    _maxTimeOpenForWriting = PackConfig.getHdfsWalMaxTimeOpenForWriting(HdfsKeyValueStore.DEFAULT_MAX_OPEN_FOR_WRITING);
    _hdfsKeyValueTimer = new Timer(HDFS_WAL_TIMER, false);
  }

  @Override
  public PackWalWriter createPackWalWriter(String name, PackMetaData metaData, WriteBlockMonitor writeBlockMonitor,
      BroadcastServerManager serverStatusManager) throws IOException {
    Path path = new Path(_root, name);
    return new PackWalWriter(name, writeBlockMonitor, serverStatusManager) {

      private TransId transId;

      @Override
      protected synchronized void writeBlocks(Blocks blocks) throws IOException {
        HdfsKeyValueStore store = getStore(path);
        transId = store.put(getKey(store), new BytesRef(Blocks.toBytes(blocks)));
      }

      private BytesRef getKey(HdfsKeyValueStore store) throws IOException {
        BytesRef lastKey = store.lastKey();
        if (lastKey == null) {
          return new BytesRef(1L);
        }
        long key = PackUtils.getLong(lastKey.bytes, 0);
        return new BytesRef(key + 1L);
      }

      @Override
      protected void internalFlush() throws IOException {
        getStore(path).sync(transId);
      }

      @Override
      protected void internalClose() throws IOException {
        closeStore(path);
      }
    };
  }

  @Override
  public PackWalReader createPackWalReader(String name, PackMetaData metaData, WalCacheManager walCacheManager,
      MaxBlockLayer maxBlockLayer) throws IOException {
    Path path = new Path(_root, name);
    return new PackWalReader(name, walCacheManager) {

      private final AtomicLong _lastOffset = new AtomicLong(-1L);

      @Override
      protected void writeDataToWal(WalCacheManager walCacheManager) throws IOException {
        while (isRunning()) {
          HdfsKeyValueStore store = getStore(path);
          cleanupOldData(store);

          Iterable<Entry<BytesRef, BytesRef>> scan = store.scan(new BytesRef(_lastOffset.get()));

          for (Entry<BytesRef, BytesRef> e : scan) {
            BytesRef key = e.getKey();
            long offset = PackUtils.getLong(key.bytes, key.offset);
            BytesRef value = e.getValue();

            Blocks blocks = Blocks.toBlocks(getBytes(value));
            // LOGGER.info("read blocks {} {} {}", Md5Utils.md5AsBase64(bs),
            // blocks.hashCode(), _zkPath + "/" + s);
            List<Block> blocksList = blocks.getBlocks();
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
      }

      private byte[] getBytes(BytesRef value) {
        // TODO Auto-generated method stub
        return null;
      }

      private void cleanupOldData(HdfsKeyValueStore store) {
        // TODO Auto-generated method stub

      }

      @Override
      public void sync() throws IOException {

      }
    };
  }

  private void closeStore(Path path) {

  }

  private HdfsKeyValueStore getStore(Path path) {
    // check ownership

    return null;
  }

  private HdfsKeyValueStore createHdfsKeyValueStore(Path path) throws IOException {
    return new HdfsKeyValueStore(false, _hdfsKeyValueTimer, _configuration, path, _maxAmountAllowedPerFile,
        _maxTimeOpenForWriting);
  }

}
