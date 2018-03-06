package pack.distributed.storage.wal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import pack.distributed.storage.hdfs.BlockFile.WriterOrdered;
import pack.distributed.storage.hdfs.CommitFile;
import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.hdfs.ReadRequest;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCacheManager implements BlockReader {

  private final static Logger LOGGER = LoggerFactory.getLogger(PackWalCacheManager.class);

  private final File _cacheDir;
  private final PackHdfsReader _hdfsReader;
  private final AtomicReference<WalCache> _currentWalCache = new AtomicReference<>();
  private final AtomicReference<List<WalCache>> _currentWalCacheReaderList = new AtomicReference<>();
  private final Cache<Long, WalCache> _walCache;
  private final long _maxWalTime = TimeUnit.MINUTES.toMillis(1);
  private final PackMetaData _metaData;
  private final Object _currentWalCacheLock = new Object();

  private FileSystem _fileSystem;

  private String _root;

  private int _blockSize;

  public PackWalCacheManager(String volumeName, File cacheDir, PackHdfsReader hdfsReader, PackMetaData metaData) {
    _metaData = metaData;
    _cacheDir = cacheDir;
    _hdfsReader = hdfsReader;
    RemovalListener<Long, WalCache> readerListener = n -> PackUtils.closeQuietly(n.getValue());
    _walCache = CacheBuilder.newBuilder()
                            .removalListener(readerListener)
                            .build();
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

  public void write(long layer, int blockId, ByteBuffer byteBuffer) throws IOException {
    WalCache walCache = getCurrentWalCache(layer);
    walCache.write(layer, blockId, byteBuffer);
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

    for (WalCache cache : list) {
      Path path = new Path(_root, UUID.randomUUID()
                                      .toString()
          + ".tmp");
      Path commit = new Path(_root, cache.getMaxLayer() + ".block");
      CommitFile commitFile = () -> {
        if (!_fileSystem.rename(path, commit)) {
          throw new IOException("Could not commit file " + commit);
        }
        LOGGER.info("Block file added {}", commit);
      };
      try (WriterOrdered writer = BlockFile.createOrdered(_fileSystem, path, _blockSize, commitFile)) {
        cache.copy(writer);
      }
    }
    _hdfsReader.refresh();
    removeOldWalCache();
  }

  private void removeOldWalCache() {
    // invalidate old entries here
    long maxLayer = _hdfsReader.getMaxLayer();

    List<Long> walIdsToInvalidate = new ArrayList<>();
    for (Entry<Long, WalCache> e : _walCache.asMap()
                                            .entrySet()) {
      WalCache cache = e.getValue();
      if (cache.getMaxLayer() < maxLayer) {
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
        walCache = new WalCache(_cacheDir, layer, _metaData.getLength(), _metaData.getBlockSize());
        _walCache.put(layer, walCache);
        _currentWalCache.set(walCache);
        updateFromReadList(walCache, false);
      }
      return walCache;
    }
  }

  private boolean shouldRollWal(WalCache walCache) {
    if (walCache.getCreationTime() + _maxWalTime < System.currentTimeMillis()) {
      return true;
    }
    return false;
  }

}
