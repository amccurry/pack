package pack.iscsi.external.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.util.IOUtils;

public class LocalBlockStore implements BlockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockStore.class);

  private final File _blockStoreDir;
  private final LoadingCache<Long, Raf> _blockGenerations;

  public LocalBlockStore(File blockStoreDir) {
    _blockStoreDir = blockStoreDir;
    _blockStoreDir.mkdirs();

    CacheLoader<Long, Raf> loader = key -> {
      File file = new File(_blockStoreDir, Long.toString(key));
      RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
      int blockSize = getBlockSize(key);
      long lengthInBytes = getLengthInBytes(key);
      long length = ((lengthInBytes / blockSize) + 1) * 8;
      randomAccessFile.setLength(length);
      return new Raf(randomAccessFile, randomAccessFile.getChannel());
    };

    RemovalListener<Long, Raf> removalListener = (key, value, cause) -> {
      if (value != null) {
        IOUtils.close(LOGGER, value);
      }
    };

    _blockGenerations = Caffeine.newBuilder()
                                .removalListener(removalListener)
                                .build(loader);
  }

  static class Raf implements Closeable {

    Raf(RandomAccessFile file, FileChannel channel) {
      _file = file;
      _channel = channel;
    }

    RandomAccessFile _file;
    FileChannel _channel;

    @Override
    public void close() throws IOException {
      _channel.close();
      _file.close();
    }
  }

  @Override
  public List<String> getVolumeNames() {
    return Arrays.asList(_blockStoreDir.list());
  }

  @Override
  public long getVolumeId(String name) {
    return Long.parseLong(name);
  }

  @Override
  public int getBlockSize(long volumeId) {
    return 64_000_000;
  }

  @Override
  public long getLengthInBytes(long volumeId) {
    return 100_000_000_000L;
  }

  @Override
  public long getLastStoreGeneration(long volumeId, long blockId) throws IOException {
    Raf raf = _blockGenerations.get(volumeId);
    ByteBuffer buffer = ByteBuffer.allocate(8);
    long position = blockId * 8;
    while (buffer.remaining() > 0) {
      position += raf._channel.read(buffer, position);
    }
    buffer.flip();
    return buffer.getLong();
  }

  @Override
  public void setLastStoreGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
    Raf raf = _blockGenerations.get(volumeId);
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(lastStoredGeneration);
    buffer.flip();
    long position = blockId * 8;
    while (buffer.remaining() > 0) {
      position += raf._channel.write(buffer, position);
    }
  }

}
