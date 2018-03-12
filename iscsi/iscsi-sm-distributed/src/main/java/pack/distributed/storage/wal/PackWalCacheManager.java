package pack.distributed.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;

import pack.distributed.storage.BlockReader;
import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.BlockFile;
import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.CommitFile;
import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.hdfs.ReadRequest;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.trace.TraceHdfsBlockReader;
import pack.distributed.storage.trace.TraceWalCache;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCacheManager implements Closeable, WalCacheManager {

  private static final String BLOCK = "block";

  private final static Logger LOGGER = LoggerFactory.getLogger(PackWalCacheManager.class);

  private final PackHdfsReader _hdfsReader;
  private final AtomicReference<WalCache> _currentWalCache = new AtomicReference<>();
  private final AtomicReference<List<WalCache>> _currentWalCacheReaderList = new AtomicReference<>(ImmutableList.of());
  private final Cache<Long, WalCache> _walCache;
  private final PackMetaData _metaData;
  private final Object _currentWalCacheLock = new Object();
  private final Configuration _configuration;
  private final Path _volumeDir;
  private final AtomicBoolean _forceRoll = new AtomicBoolean(false);
  private final WriteBlockMonitor _writeBlockMonitor;
  private final WalCacheFactory _cacheFactory;
  private final long _maxWalSize;

  public PackWalCacheManager(String volumeName, WriteBlockMonitor writeBlockMonitor, WalCacheFactory cacheFactory,
      PackHdfsReader hdfsReader, PackMetaData metaData, Configuration configuration, Path volumeDir, long maxWalSize) {
    _maxWalSize = maxWalSize;
    _cacheFactory = cacheFactory;
    _writeBlockMonitor = writeBlockMonitor;
    _volumeDir = volumeDir;
    _configuration = configuration;
    _metaData = metaData;
    _hdfsReader = hdfsReader;
    RemovalListener<Long, WalCache> readerListener = n -> PackUtils.closeQuietly(n.getValue());
    _walCache = CacheBuilder.newBuilder()
                            .removalListener(readerListener)
                            .build();
  }

  public long getMaxLayer() {
    WalCache walCache = _currentWalCache.get();
    if (walCache == null) {
      return -1L;
    }
    return walCache.getMaxLayer();
  }

  @Override
  public void close() throws IOException {
    _walCache.invalidateAll();
  }

  @Override
  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    List<WalCache> list = _currentWalCacheReaderList.get();
    if (list.isEmpty()) {
      return true;
    }
    return BlockReader.mergeInOrder(list)
                      .readBlocks(requests);
  }

  @Override
  public List<BlockReader> getLeaves() {
    List<WalCache> list = _currentWalCacheReaderList.get();
    return new ArrayList<>(list);
  }

  @Override
  public void write(long transId, long layer, int blockId, byte[] value) throws IOException {
    WalCache walCache = getCurrentWalCache(layer);
    walCache.write(layer, blockId, value);
    _writeBlockMonitor.resetDirtyBlock(blockId, transId);
  }

  public void writeWalCacheToHdfs() throws IOException {
    WalCache walCache = _currentWalCache.get();
    List<WalCache> list = new ArrayList<>();
    for (Entry<Long, WalCache> e : _walCache.asMap()
                                            .entrySet()) {
      WalCache cache = e.getValue();
      if (cache != walCache) {
        list.add(cache);
      }
    }
    if (list.isEmpty()) {
      return;
    }
    Collections.sort(list);

    // write oldest first
    Collections.reverse(list);

    Path blockDir = new Path(_volumeDir, BLOCK);
    FileSystem fileSystem = blockDir.getFileSystem(_configuration);
    for (WalCache cache : list) {
      String uuid = UUID.randomUUID()
                        .toString();
      Path path = fileSystem.makeQualified(new Path(blockDir, uuid + ".tmp"));
      Path commit = fileSystem.makeQualified(new Path(blockDir, cache.getMaxLayer() + ".block"));
      CommitFile commitFile = () -> {
        if (!fileSystem.rename(path, commit)) {
          throw new IOException("Could not commit file " + commit);
        }
        LOGGER.info("Block file added {}", commit);
      };
      try (Writer writer = TraceHdfsBlockReader.traceIfEnabled(
          BlockFile.createOrdered(fileSystem, path, _metaData.getBlockSize(), commitFile), commit)) {
        cache.copy(writer);
      }
    }
    removeOldWalCache();
  }

  public void removeOldWalCache() throws IOException {
    _hdfsReader.refresh();
    // invalidate old entries here
    long maxLayer = _hdfsReader.getMaxLayer();

    List<Long> walIdsToInvalidate = new ArrayList<>();
    for (Entry<Long, WalCache> e : _walCache.asMap()
                                            .entrySet()) {
      WalCache cache = e.getValue();
      if (cache.getMaxLayer() <= maxLayer) {
        walIdsToInvalidate.add(e.getKey());
        updateFromReadList(cache, true);
      }
    }
    for (Long id : walIdsToInvalidate) {
      LOGGER.info("Removing old wal cache {}", id);
      _walCache.invalidate(id);
    }
  }

  private void updateFromReadList(WalCache walCache, boolean remove) {
    List<WalCache> list = new ArrayList<>(_currentWalCacheReaderList.get());
    if (remove) {
      list.remove(walCache);
    } else {
      list.add(walCache);
    }
    Collections.sort(list);
    if (list.isEmpty()) {
      _currentWalCacheReaderList.set(ImmutableList.of());
    } else {
      _currentWalCacheReaderList.set(ImmutableList.copyOf(list));
    }
  }

  private WalCache getCurrentWalCache(long layer) throws IOException {
    synchronized (_currentWalCacheLock) {
      WalCache walCache = _currentWalCache.get();
      if (shouldRollWal(walCache)) {
        walCache = TraceWalCache.traceIfEnabled(_cacheFactory.create(layer));
        _walCache.put(layer, walCache);
        _currentWalCache.set(walCache);
        updateFromReadList(walCache, false);
      }
      return walCache;
    }
  }

  private boolean shouldRollWal(WalCache walCache) {
    if (walCache == null) {
      return true;
    } else if (_forceRoll.get()) {
      _forceRoll.set(false);
      return true;
    } else if (walCache.getSize() >= _maxWalSize) {
      return true;
    } else {
      return false;
    }
  }

  public void forceRollOnNextWrite() {
    _forceRoll.set(true);
  }

}
