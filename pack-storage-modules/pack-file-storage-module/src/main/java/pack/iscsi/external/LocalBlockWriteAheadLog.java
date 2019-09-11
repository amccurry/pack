package pack.iscsi.external;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.util.IOUtils;

public class LocalBlockWriteAheadLog implements BlockWriteAheadLog {

  @EqualsAndHashCode
  @Builder
  @Value
  static class LogKey {
    long volumeId;
    long blockId;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockWriteAheadLog.class);

  private final LoadingCache<LogKey, WriteAheadLogger> _cache;
  private final File _walLogDir;

  public LocalBlockWriteAheadLog(File walLogDir) throws IOException {
    _walLogDir = walLogDir;
    _walLogDir.mkdirs();
    if (!walLogDir.exists()) {
      throw new IOException("dir " + walLogDir + " does not exist");
    }
    CacheLoader<LogKey, WriteAheadLogger> loader = key -> {
      File blockLogDir = getBlockLogDir(key);
      blockLogDir.mkdirs();
      long volumeId = key.getVolumeId();
      long blockId = key.getBlockId();
      return new LocalWriteAheadLogger(blockLogDir, volumeId, blockId);
    };
    RemovalListener<LogKey, WriteAheadLogger> removalListener = (key, log, cause) -> IOUtils.close(LOGGER, log);
    _cache = Caffeine.newBuilder()
                     .expireAfterWrite(10, TimeUnit.SECONDS)
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    WriteAheadLogger log = getLog(volumeId, blockId);
    return () -> log.append(generation, position, bytes, offset, len);
  }

  @Override
  public void release(long volumeId, long blockId, long generation) throws IOException {
    WriteAheadLogger log = getLog(volumeId, blockId);
    log.release(generation);
  }

  @Override
  public long recover(FileChannel channel, long volumeId, long blockId, long onDiskGeneration) throws IOException {
    WriteAheadLogger log = getLog(volumeId, blockId);
    return log.recover(channel, onDiskGeneration);
  }

  private WriteAheadLogger getLog(long volumeId, long blockId) {
    return _cache.get(LogKey.builder()
                            .blockId(blockId)
                            .volumeId(volumeId)
                            .build());
  }

  private File getBlockLogDir(LogKey key) {
    File volumeDir = new File(_walLogDir, Long.toString(key.getVolumeId()));
    File blockDir = new File(volumeDir, Long.toString(key.getBlockId()));
    return blockDir;
  }

}
