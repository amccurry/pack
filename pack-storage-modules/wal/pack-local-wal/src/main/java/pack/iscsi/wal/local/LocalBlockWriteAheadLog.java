package pack.iscsi.wal.local;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.util.ExecutorUtil;

public class LocalBlockWriteAheadLog implements BlockWriteAheadLog {

  @Value
  @Builder(toBuilder = true)
  public static class LocalBlockWriteAheadLogConfig {

    File walLogDir;

    @Builder.Default
    Executor executor = Executors.newSingleThreadExecutor();
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
  private final Executor _asyncExecutor;

  public LocalBlockWriteAheadLog(LocalBlockWriteAheadLogConfig config) throws IOException {
    _asyncExecutor = config.getExecutor();
    _walLogDir = config.getWalLogDir();
    _walLogDir.mkdirs();
    if (!_walLogDir.exists()) {
      throw new IOException("dir " + _walLogDir + " does not exist");
    }
    CacheLoader<JournalKey, LocalJournal> loader = key -> {
      File blockLogDir = getBlockLogDir(key);
      long volumeId = key.getVolumeId();
      long blockId = key.getBlockId();
      return new LocalJournal(blockLogDir, volumeId, blockId);
    };
    RemovalListener<JournalKey, LocalJournal> removalListener = (key, log, cause) -> IOUtils.close(LOGGER, log);

    _cache = Caffeine.newBuilder()
                     .executor(ExecutorUtil.getCallerRunExecutor())
                     .expireAfterWrite(10, TimeUnit.SECONDS)
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public AsyncCompletableFuture write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    LocalJournal journal = getJournal(volumeId, blockId);
    return AsyncCompletableFuture.exec(LocalBlockWriteAheadLog.class, "append", _asyncExecutor, () -> {
      synchronized (journal) {
        journal.append(generation, position, bytes, offset, len);
      }
    });
  }

  @Override
  public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {
    LocalJournal journal = getJournal(volumeId, blockId);
    synchronized (journal) {
      journal.release(generation);
    }
  }

  @Override
  public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
      boolean closeExistingWriter) throws IOException {
    LocalJournal journal = getJournal(volumeId, blockId);
    synchronized (journal) {
      return journal.getJournalRanges(onDiskGeneration, closeExistingWriter);
    }
  }

  @Override
  public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
      throws IOException {
    LocalJournal journal = getJournal(range.getVolumeId(), range.getBlockId());
    synchronized (journal) {
      return journal.recover(range.getUuid(), writer, onDiskGeneration);
    }
  }

  private synchronized LocalJournal getJournal(long volumeId, long blockId) {
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
