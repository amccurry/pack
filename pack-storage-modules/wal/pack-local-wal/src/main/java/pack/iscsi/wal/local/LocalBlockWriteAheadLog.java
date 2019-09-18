package pack.iscsi.wal.local;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockJournalResult;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.spi.wal.BlockWriteAheadLog;

public class LocalBlockWriteAheadLog implements BlockWriteAheadLog {

  @Value
  @Builder(toBuilder = true)
  public static class LocalBlockWriteAheadLogConfig {
    File walLogDir;
  }

  @EqualsAndHashCode
  @Builder
  @Value
  static class JournalKey {
    long volumeId;
    long blockId;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockWriteAheadLog.class);

  private final LoadingCache<JournalKey, LocalJournal> _cache;
  private final File _walLogDir;

  public LocalBlockWriteAheadLog(LocalBlockWriteAheadLogConfig config) throws IOException {
    _walLogDir = config.getWalLogDir();
    _walLogDir.mkdirs();
    if (!_walLogDir.exists()) {
      throw new IOException("dir " + _walLogDir + " does not exist");
    }
    CacheLoader<JournalKey, LocalJournal> loader = key -> {
      File blockLogDir = getBlockLogDir(key);
      blockLogDir.mkdirs();
      long volumeId = key.getVolumeId();
      long blockId = key.getBlockId();
      return new LocalJournal(blockLogDir, volumeId, blockId);
    };
    RemovalListener<JournalKey, LocalJournal> removalListener = (key, log, cause) -> IOUtils.close(LOGGER, log);
    _cache = Caffeine.newBuilder()
                     .expireAfterWrite(10, TimeUnit.SECONDS)
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public BlockJournalResult write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset,
      int len) throws IOException {
    LocalJournal journal = getJournal(volumeId, blockId);
    return () -> journal.append(generation, position, bytes, offset, len);
  }

  @Override
  public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {
    LocalJournal journal = getJournal(volumeId, blockId);
    journal.release(generation);
  }

  @Override
  public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
      boolean closeExistingWriter) throws IOException {
    LocalJournal log = getJournal(volumeId, blockId);
    return log.getJournalRanges(onDiskGeneration, closeExistingWriter);
  }

  @Override
  public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
      throws IOException {
    LocalJournal journal = getJournal(range.getVolumeId(), range.getBlockId());
    return journal.recover(range.getUuid(), writer, onDiskGeneration);
  }

  private LocalJournal getJournal(long volumeId, long blockId) {
    return _cache.get(JournalKey.builder()
                                .blockId(blockId)
                                .volumeId(volumeId)
                                .build());
  }

  private File getBlockLogDir(JournalKey key) {
    File volumeDir = new File(_walLogDir, Long.toString(key.getVolumeId()));
    File blockDir = new File(volumeDir, Long.toString(key.getBlockId()));
    return blockDir;
  }

}
