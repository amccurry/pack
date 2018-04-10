package pack.distributed.storage.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.PackConfig;
import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.kvs.BytesRef;
import pack.distributed.storage.hdfs.kvs.HdfsKeyValueStore;
import pack.distributed.storage.hdfs.kvs.KeyValueStore;
import pack.distributed.storage.hdfs.kvs.KeyValueStoreTransId;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(PackHdfsWalFactory.class);

  private static final String HDFS_WAL_TIMER = "hdfs-wal-timer";

  private final Timer _hdfsKeyValueTimer;
  private final Configuration _configuration;
  private final Path _root;
  private final long _maxAmountAllowedPerFile;
  private final long _maxTimeOpenForWriting;
  private final Map<Path, KeyValueStore> _storeMap = new ConcurrentHashMap<>();

  public PackHdfsWalFactory(Configuration configuration, Path rootWal) throws IOException {
    _configuration = configuration;
    FileSystem fileSystem = rootWal.getFileSystem(configuration);
    _root = rootWal.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());

    _maxAmountAllowedPerFile = PackConfig.getHdfsWalMaxAmountAllowedPerFile(
        HdfsKeyValueStore.DEFAULT_MAX_AMOUNT_ALLOWED_PER_FILE);
    _maxTimeOpenForWriting = PackConfig.getHdfsWalMaxTimeOpenForWriting(HdfsKeyValueStore.DEFAULT_MAX_OPEN_FOR_WRITING);
    _hdfsKeyValueTimer = new Timer(HDFS_WAL_TIMER, true);
  }

  @Override
  public PackWalWriter createPackWalWriter(String name, PackMetaData metaData, WriteBlockMonitor writeBlockMonitor,
      BroadcastServerManager serverStatusManager) throws IOException {
    Path path = new Path(_root, name);
    return new PackWalWriter(name, writeBlockMonitor, serverStatusManager) {

      private KeyValueStoreTransId transId;

      @Override
      protected synchronized void writeBlocks(Blocks blocks) throws IOException {
        KeyValueStore store = getStore(path);
        transId = store.put(getKey(store), new BytesRef(Blocks.toBytes(blocks)));
      }

      private BytesRef getKey(KeyValueStore store) throws IOException {
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
          KeyValueStore store = getStore(path);
          cleanupOldData(store);
          Iterable<Entry<BytesRef, BytesRef>> scan = store.scan(getScanStart());
          boolean empty = true;
          for (Entry<BytesRef, BytesRef> e : scan) {
            empty = false;
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
          if (empty) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }

      private BytesRef getScanStart() {
        long l = _lastOffset.get();
        return new BytesRef(l + 1);
      }

      private void cleanupOldData(KeyValueStore store) throws IOException {
        long maxLayer = maxBlockLayer.getMaxLayer();
        if (maxLayer == 0L) {
          return;
        }
        store.deleteRange(BytesRef.value(0L), BytesRef.value(maxLayer));
      }

      @Override
      public void sync() throws IOException {
        KeyValueStore store = getStore(path);
        long sleep = 1;
        while (true) {
          BytesRef lastKey = store.lastKey();
          if (lastKey != null) {
            long offset = PackUtils.getLong(lastKey.bytes, lastKey.offset);
            LOGGER.info("lastKey {} offset {}", lastKey, offset);
            if (offset == _lastOffset.get()) {
              return;
            }
          } else {
            LOGGER.info("lastKey {}", lastKey);
          }
          if (sleep < 1000) {
            sleep++;
          }
          try {
            Thread.sleep(sleep);
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
      }

      private byte[] getBytes(BytesRef value) {
        if (value.offset == 0 && value.length == value.bytes.length) {
          return value.bytes;
        }
        byte[] bs = new byte[value.length];
        System.arraycopy(value.bytes, value.offset, bs, 0, bs.length);
        return bs;
      }
    };
  }

  private void closeStore(Path path) {
    PackUtils.close(LOGGER, _storeMap.remove(path));
  }

  private KeyValueStore getStore(Path path) throws IOException {
    KeyValueStore store = _storeMap.get(path);
    if (store == null) {
      return newStore(path);
    }
    return store;
  }

  private synchronized KeyValueStore newStore(Path path) throws IOException {
    KeyValueStore store = _storeMap.get(path);
    if (store == null) {
      _storeMap.put(path, store = createHdfsKeyValueStore(path));
    }
    return store;
  }

  private HdfsKeyValueStore createHdfsKeyValueStore(Path path) throws IOException {
    return new HdfsKeyValueStore(false, _hdfsKeyValueTimer, _configuration, path, _maxAmountAllowedPerFile,
        _maxTimeOpenForWriting);
  }

  @Override
  public void close() throws IOException {
    _hdfsKeyValueTimer.purge();
    _hdfsKeyValueTimer.cancel();
  }

}
