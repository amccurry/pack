package pack.distributed.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.BlockFile;
import pack.distributed.storage.hdfs.BlockFile.Writer;
import pack.distributed.storage.hdfs.CommitFile;
import pack.distributed.storage.hdfs.PackHdfsReader;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.read.BlockReader;
import pack.distributed.storage.read.ReadRequest;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.trace.TraceHdfsBlockReader;
import pack.distributed.storage.trace.TraceWalCache;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCacheManager implements Closeable, WalCacheManager {

  private static final String BLOCK = "block";

  private final static Logger LOGGER = LoggerFactory.getLogger(PackWalCacheManager.class);

  private final PackHdfsReader _hdfsReader;
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
  private final Thread _updateHdfsThread;
  private final Object _updateHdfsLock = new Object();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final String _volumeName;
  private final long _maxWalLifeTime;
  private final ServerStatusManager _serverStatusManager;
  private final Set<WalCache> _toBeClosed = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Thread _closeOldWalFiles;
  private final Object _closeOldWalFileLock = new Object();

  public PackWalCacheManager(String volumeName, WriteBlockMonitor writeBlockMonitor, WalCacheFactory cacheFactory,
      PackHdfsReader hdfsReader, ServerStatusManager serverStatusManager, PackMetaData metaData,
      Configuration configuration, Path volumeDir, long maxWalSize, long maxWalLifeTime) {
    this(volumeName, writeBlockMonitor, cacheFactory, hdfsReader, serverStatusManager, metaData, configuration,
        volumeDir, maxWalSize, maxWalLifeTime, true);
  }

  public PackWalCacheManager(String volumeName, WriteBlockMonitor writeBlockMonitor, WalCacheFactory cacheFactory,
      PackHdfsReader hdfsReader, ServerStatusManager serverStatusManager, PackMetaData metaData,
      Configuration configuration, Path volumeDir, long maxWalSize, long maxWalLifeTime, boolean enableAutoHdfsWrite) {
    _serverStatusManager = serverStatusManager;
    _maxWalLifeTime = maxWalLifeTime;
    _volumeName = volumeName;
    _maxWalSize = maxWalSize;
    _cacheFactory = cacheFactory;
    _writeBlockMonitor = writeBlockMonitor;
    _volumeDir = volumeDir;
    _configuration = configuration;
    _metaData = metaData;
    _hdfsReader = hdfsReader;
    RemovalListener<Long, WalCache> readerListener = notification -> _toBeClosed.add(notification.getValue());
    _walCache = CacheBuilder.newBuilder()
                            .removalListener(readerListener)
                            .build();
    _updateHdfsThread = createUpdateHdfsThread(volumeName);
    if (enableAutoHdfsWrite) {
      _updateHdfsThread.start();
    }
    _closeOldWalFiles = createCloseOldWalFileThread(volumeName);
    _closeOldWalFiles.start();
  }

  private Thread createCloseOldWalFileThread(String volumeName) {
    Thread thread = new Thread(() -> {
      closeOldWalFilesAsync();
    });
    thread.setName("close-old-wal-" + volumeName);
    thread.setDaemon(true);
    return thread;
  }

  private void closeOldWalFilesAsync() {
    while (_running.get()) {
      synchronized (_closeOldWalFileLock) {
        try {
          closeOldWalFiles();
        } catch (Throwable t) {
          LOGGER.error("Unknown error while closing old wal files", t);
        }
        try {
          _closeOldWalFileLock.wait();
        } catch (InterruptedException e) {
          LOGGER.error("Unknown error", e);
        }
      }
    }
  }

  private void startCloseOldWalFiles() {
    synchronized (_closeOldWalFileLock) {
      _closeOldWalFileLock.notifyAll();
    }
  }

  private void closeOldWalFiles() {
    Iterator<WalCache> iterator = _toBeClosed.iterator();
    while (iterator.hasNext()) {
      WalCache walCache = iterator.next();
      if (walCache.refCount() == 0) {
        LOGGER.info("Removing old wal file {} {}", _volumeName, walCache.getId());
        iterator.remove();
        PackUtils.close(LOGGER, walCache);
      }
    }
  }

  private Thread createUpdateHdfsThread(String volumeName) {
    Thread thread = new Thread(() -> {
      updateHdfsAsync();
    });
    thread.setName("hdfs-update-" + volumeName);
    thread.setDaemon(true);
    return thread;
  }

  private void updateHdfsAsync() {
    while (_running.get()) {
      synchronized (_updateHdfsLock) {
        try {
          updateHdfs();
        } catch (Throwable t) {
          LOGGER.error("Unknown error while updating hdfs", t);
        }
        try {
          _updateHdfsLock.wait();
        } catch (InterruptedException e) {
          LOGGER.error("Unknown error", e);
        }
      }
    }
  }

  private void startHdfsUpdate() {
    synchronized (_updateHdfsLock) {
      _updateHdfsLock.notifyAll();
    }
  }

  public void updateHdfs() throws IOException {
    LOGGER.debug("Updating hdfs {}", _volumeName);
    if (_serverStatusManager.isLeader(_volumeName)) {
      writeWalCacheToHdfs();
    } else {
      removeOldWalCache();
    }
  }

  public long getMaxLayer() {
    WalCache walCache = getCurrentWalCache();
    if (walCache == null) {
      return -1L;
    }
    return walCache.getMaxLayer();
  }

  private WalCache getCurrentWalCache() {
    List<WalCache> list;
    synchronized (_currentWalCacheLock) {
      list = _currentWalCacheReaderList.get();
    }
    if (list == null || list.isEmpty()) {
      return null;
    } else {
      return list.get(0);
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _updateHdfsThread.interrupt();
    _walCache.invalidateAll();
  }

  @Override
  public BlockReader getBlockReader() throws IOException {
    List<WalCache> list = _currentWalCacheReaderList.get();
    incrementRefs(list);
    return new BlockReader() {
      @Override
      public boolean readBlocks(List<ReadRequest> requests) throws IOException {
        if (list.isEmpty()) {
          return true;
        }
        return BlockReader.mergeInOrder(list)
                          .readBlocks(requests);
      }

      @Override
      public List<BlockReader> getLeaves() {
        return new ArrayList<>(list);
      }

      @Override
      public void close() throws IOException {
        decrementRefs(list);
        startCloseOldWalFiles();
      }

    };
  }

  private void decrementRefs(List<WalCache> list) {
    for (WalCache walCache : list) {
      walCache.decRef();
    }
  }

  private void incrementRefs(List<WalCache> list) {
    for (WalCache walCache : list) {
      walCache.incRef();
    }
  }

  @Override
  public void write(long transId, long layer, int blockId, byte[] value) throws IOException {
    WalCache walCache = getCurrentWalCache(layer);
    walCache.write(layer, blockId, value);
    _writeBlockMonitor.resetDirtyBlock(blockId, transId);
  }

  public void writeWalCacheToHdfs() throws IOException {
    LOGGER.debug("Writing wal cache to hdfs {}", _volumeName);
    WalCache walCache = getCurrentWalCache();
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
          if (fileSystem.exists(commit)) {
            fileSystem.delete(path, false);
            LOGGER.info("File {} was already written", commit);
            return;
          } else {
            throw new IOException("Could not commit file " + commit);
          }
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
    LOGGER.debug("Removing old wal cache {}", _volumeName);
    _hdfsReader.refresh();
    // invalidate old entries here
    long maxLayer = _hdfsReader.getMaxLayer();

    List<Long> walIdsToInvalidate = new ArrayList<>();
    WalCache current = getCurrentWalCache();
    for (Entry<Long, WalCache> e : _walCache.asMap()
                                            .entrySet()) {
      WalCache cache = e.getValue();
      if (cache == current) {
        continue;
      } else if (cache.getMaxLayer() <= maxLayer) {
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
    synchronized (_currentWalCacheLock) {
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
  }

  private WalCache getCurrentWalCache(long layer) throws IOException {
    WalCache walCache = getCurrentWalCache();
    if (shouldRollWal(walCache)) {
      walCache = TraceWalCache.traceIfEnabled(_cacheFactory.create(layer));
      _walCache.put(layer, walCache);
      updateFromReadList(walCache, false);
      startHdfsUpdate();
    }
    return walCache;
  }

  private boolean shouldRollWal(WalCache walCache) {
    if (walCache == null) {
      return true;
    } else if (walCache.isClosed()) {
      return true;
    } else if (_forceRoll.get()) {
      _forceRoll.set(false);
      return true;
    } else if (walCache.getSize() >= _maxWalSize
        || walCache.getCreationTime() + _maxWalLifeTime < System.currentTimeMillis()) {
      return true;
    } else {
      return false;
    }
  }

  public void forceRollOnNextWrite() {
    _forceRoll.set(true);
  }

}
