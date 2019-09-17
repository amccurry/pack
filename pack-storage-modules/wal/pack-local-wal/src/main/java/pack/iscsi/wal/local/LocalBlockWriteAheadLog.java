package pack.iscsi.wal.local;

import java.io.File;
import java.io.IOException;
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
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;

public class LocalBlockWriteAheadLog implements BlockWriteAheadLog {

  @Value
  @Builder(toBuilder = true)
  public static class LocalBlockWriteAheadLogConfig {
    File walLogDir;
  }

  @EqualsAndHashCode
  @Builder
  @Value
  static class LogKey {
    long volumeId;
    long blockId;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockWriteAheadLog.class);

  private final LoadingCache<LogKey, LocalWriteAheadLogger> _cache;
  private final File _walLogDir;

  public LocalBlockWriteAheadLog(LocalBlockWriteAheadLogConfig config) throws IOException {
    _walLogDir = config.getWalLogDir();
    _walLogDir.mkdirs();
    if (!_walLogDir.exists()) {
      throw new IOException("dir " + _walLogDir + " does not exist");
    }
    CacheLoader<LogKey, LocalWriteAheadLogger> loader = key -> {
      File blockLogDir = getBlockLogDir(key);
      blockLogDir.mkdirs();
      long volumeId = key.getVolumeId();
      long blockId = key.getBlockId();
      return new LocalWriteAheadLogger(blockLogDir, volumeId, blockId);
    };
    RemovalListener<LogKey, LocalWriteAheadLogger> removalListener = (key, log, cause) -> IOUtils.close(LOGGER, log);
    _cache = Caffeine.newBuilder()
                     .expireAfterWrite(10, TimeUnit.SECONDS)
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    LocalWriteAheadLogger log = getLog(volumeId, blockId);
    return () -> log.append(generation, position, bytes, offset, len);
  }

  @Override
  public void release(long volumeId, long blockId, long generation) throws IOException {
    LocalWriteAheadLogger log = getLog(volumeId, blockId);
    log.release(generation);
  }

  public long getMaxGeneration(long volumeId, long blockId) throws IOException {
    LocalWriteAheadLogger log = getLog(volumeId, blockId);
    return log.getMaxGeneration();
  }

  @Override
  public long recover(RandomAccessIO randomAccessIO, long volumeId, long blockId, long onDiskGeneration)
      throws IOException {
    LocalWriteAheadLogger log = getLog(volumeId, blockId);
    return log.recover(randomAccessIO, onDiskGeneration);
  }

  private LocalWriteAheadLogger getLog(long volumeId, long blockId) {
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
