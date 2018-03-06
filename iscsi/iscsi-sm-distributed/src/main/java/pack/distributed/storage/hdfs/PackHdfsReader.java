package pack.distributed.storage.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
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

import pack.distributed.storage.BlockReader;
import pack.distributed.storage.hdfs.BlockFile.Reader;
import pack.iscsi.storage.utils.PackUtils;

public class PackHdfsReader implements BlockReader, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackHdfsReader.class);

  private static final String BLOCK = "block";
  private final Configuration _conf;
  private final Path _blockDir;
  private final UserGroupInformation _ugi;
  private final Cache<Path, BlockFile.Reader> _readerCache;
  private final AtomicReference<BlockReader> _currentBlockReader = new AtomicReference<BlockReader>(
      BlockReader.NOOP_READER);
  private final AtomicLong _maxLayer = new AtomicLong();

  public PackHdfsReader(Configuration conf, Path volumeDir, UserGroupInformation ugi) throws IOException {
    _conf = conf;
    _blockDir = new Path(volumeDir, BLOCK);
    _ugi = ugi;
    FileSystem fileSystem = _blockDir.getFileSystem(conf);
    fileSystem.mkdirs(_blockDir);
    RemovalListener<Path, BlockFile.Reader> readerListener = n -> PackUtils.closeQuietly(n.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(readerListener)
                               .build();
  }

  public void refresh() throws IOException {
    try {
      _ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FileSystem fileSystem = _blockDir.getFileSystem(_conf);
        FileStatus[] listStatus = fileSystem.listStatus(_blockDir, (PathFilter) path -> BlockFile.isOrderedBlock(path));
        Arrays.sort(listStatus, BlockFile.ORDERED_FILESTATUS_COMPARATOR);
        Builder<BlockReader> builder = ImmutableList.builder();
        Set<String> sourceBlockFiles = new HashSet<>();
        for (FileStatus blockFileStatus : listStatus) {
          Path path = blockFileStatus.getPath();
          Reader reader = getReader(path);
          if (sourceBlockFiles.contains(path.getName())) {
            LOGGER.info("No longer in use {}", path);
            _readerCache.invalidate(path);
          } else {
            updateMaxLayer(path);
            builder.add(reader);
          }
          sourceBlockFiles.addAll(reader.getSourceBlockFiles());
        }
        _currentBlockReader.set(BlockReader.mergeInOrder(builder.build()));
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public Set<Path> getIsUseReaderPathSet() {
    return _readerCache.asMap()
                       .keySet();
  }

  @Override
  public boolean readBlocks(List<ReadRequest> requests) throws IOException {
    try {
      return _ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> _currentBlockReader.get()
                                                                                     .readBlocks(requests));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    _readerCache.invalidateAll();
  }

  public long getMaxLayer() {
    return _maxLayer.get();
  }

  private BlockFile.Reader getReader(Path path) throws IOException {
    try {
      LOGGER.debug("getReader {}", path);
      return _readerCache.get(path, () -> BlockFile.open(path.getFileSystem(_conf), path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private void updateMaxLayer(Path path) {
    Long layer = BlockFile.getLayer(path);
    if (_maxLayer.get() < layer) {
      _maxLayer.set(layer);
    }
  }
}
