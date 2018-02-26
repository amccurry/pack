package pack.iscsi.storage;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.util.Utils;
import pack.iscsi.storage.hdfs.BlockFile;
import pack.iscsi.storage.hdfs.BlockFile.Reader;
import utils.IOUtils;

public class HdfsDataArchiveManager implements DataArchiveManager, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsDataArchiveManager.class);

  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final Future<Void> _blockFileLoaderFuture;
  private final Path _path;
  private final PackStorageMetaData _metaData;
  private final Cache<Path, BlockFile.Reader> _readerCache;
  private final UserGroupInformation _ugi;
  private final FileSystem _fileSystem;
  private final AtomicReference<BlockReader> _currentBlockReader = new AtomicReference<BlockReader>(
      BlockReader.NOOP_READER);

  public HdfsDataArchiveManager(PackStorageMetaData metaData, Configuration configuration, Path path,
      UserGroupInformation ugi) throws IOException, InterruptedException {
    _fileSystem = ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> path.getFileSystem(configuration));
    _metaData = metaData;
    _path = path;
    _ugi = ugi;
    RemovalListener<Path, BlockFile.Reader> readerListener = n -> IOUtils.closeQuietly(n.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(readerListener)
                               .build();
    _blockFileLoaderFuture = _executorService.submit(new BlockFileLoader());
  }

  public void checkState() {
    IOUtils.checkFutureIsRunning(_blockFileLoaderFuture);
  }

  @Override
  public BlockReader getBlockReader() throws IOException {
    return _currentBlockReader.get();
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _readerCache.invalidateAll();
    _executorService.shutdownNow();
  }

  class BlockFileLoader implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      while (_running.get()) {
        Thread.sleep(_metaData.getHdfsPollTime());
        _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          updateBlocks();
          return null;
        });
      }
      return null;
    }
  }

  public void updateBlocks() throws InterruptedException {
    try {
      List<Path> blockPathList = getBlockFilePathListFromStorage();
      loadNewReaders(blockPathList);
    } catch (IOException e) {
      LOGGER.error("Unknown error", e);
    }
  }

  private void loadNewReaders(List<Path> blockPathList) throws IOException, FileNotFoundException {
    Builder<BlockReader> builder = ImmutableList.builder();
    for (Path blockPath : blockPathList) {
      builder.add(addUgi(getReader(blockPath)));
    }
    _currentBlockReader.set(BlockReader.mergeInOrder(builder.build()));
  }

  private List<Path> getBlockFilePathListFromStorage() throws FileNotFoundException, IOException {
    Builder<Path> builder = ImmutableList.builder();
    if (!_fileSystem.exists(_path)) {
      return builder.build();
    }
    FileStatus[] listStatus = _fileSystem.listStatus(_path, (PathFilter) p -> BlockFile.isOrderedBlock(p));
    Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
    for (FileStatus fileStatus : listStatus) {
      builder.add(Utils.qualify(_fileSystem, fileStatus.getPath()));
    }
    return builder.build();
  }

  private Reader getReader(Path path) throws IOException {
    try {
      LOGGER.debug("getReader {}", path);
      return _readerCache.get(path, () -> BlockFile.open(_fileSystem, path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  public BlockReader addUgi(BlockReader reader) {
    return requests -> {
      try {
        return _ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> reader.readBlocks(requests));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    };
  }

}
