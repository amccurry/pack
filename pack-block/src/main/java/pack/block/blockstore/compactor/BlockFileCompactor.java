package pack.block.blockstore.compactor;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.blockstore.HdfsBlockStoreImplConfig;
import pack.block.blockstore.hdfs.file.BlockFile;
import pack.block.blockstore.hdfs.file.BlockFile.Reader;

public class BlockFileCompactor extends BlockFileCompactorBase implements Closeable {

  private final Path _blockPath;
  private final FileSystem _fileSystem;
  private final long _maxBlockFileSize;
  private final Cache<Path, Reader> _readerCache;
  private final double _maxObsoleteRatio;
  private final String _nodePrefix;
  private final int _fileSystemBlockSize;

  public BlockFileCompactor(FileSystem fileSystem, Path path, BlockStoreMetaData metaData) throws IOException {
    _nodePrefix = InetAddress.getLocalHost()
                             .getHostName();
    _fileSystemBlockSize = metaData.getFileSystemBlockSize();
    _maxBlockFileSize = metaData.getMaxBlockFileSize();
    _maxObsoleteRatio = metaData.getMaxObsoleteRatio();
    _fileSystem = fileSystem;
    _blockPath = new Path(path, HdfsBlockStoreImplConfig.BLOCK);
    RemovalListener<Path, BlockFile.Reader> listener = notification -> IOUtils.closeQuietly(notification.getValue());
    _readerCache = CacheBuilder.newBuilder()
                               .removalListener(listener)
                               .build();
    cleanupBlocks();
  }

  @Override
  protected FileSystem getFileSystem() throws IOException {
    return _fileSystem;
  }

  @Override
  protected Path getBlockPath() {
    return _blockPath;
  }

  @Override
  protected Reader getReader(Path path) throws IOException {
    try {
      return _readerCache.get(path, () -> BlockFile.openForStreaming(_fileSystem, path));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  @Override
  protected String getNodePrefix() {
    return _nodePrefix;
  }

  @Override
  protected double getMaxObsoleteRatio() {
    return _maxObsoleteRatio;
  }

  @Override
  protected long getMaxBlockFileSize() {
    return _maxBlockFileSize;
  }

  @Override
  public void close() throws IOException {
    _readerCache.invalidateAll();
  }

  @Override
  protected int getFileSystemBlockSize() {
    return _fileSystemBlockSize;
  }

}
