package pack.iscsi.bk.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.curator.framework.CuratorFramework;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.ImmutableMap;

import io.opencensus.common.Scope;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.util.IOUtils;
import pack.util.TracerUtil;

public class BookKeeperWriteAheadLog implements BlockWriteAheadLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(BookKeeperWriteAheadLog.class);

  private static final char SEP = '/';
  private static final String LEDGER_STORE = "ledger-store";
  private static final String VOLUME_ID = "packVolumeId";
  private static final String BLOCK_ID = "packBlockId";

  @Value
  @Builder(toBuilder = true)
  @EqualsAndHashCode
  static class LedgerHandleKey {
    long volumeId;
    long blockId;
  }

  private final BookKeeper _bookKeeper;
  private final LoadingCache<LedgerHandleKey, LedgerHandle> _cache;
  private final CuratorFramework _curatorFramework;
  private final DigestType _digestType;
  private final int _ensSize;
  private final int _writeQuorumSize;
  private final int _ackQuorumSize;

  public BookKeeperWriteAheadLog(BookKeeperWriteAheadLogConfig config) {
    _digestType = config.getDigestType();
    _ensSize = config.getEnsSize();
    _writeQuorumSize = config.getWriteQuorumSize();
    _ackQuorumSize = config.getAckQuorumSize();
    _curatorFramework = config.getCuratorFramework();
    _bookKeeper = config.getBookKeeper();
    RemovalListener<LedgerHandleKey, LedgerHandle> removalListener = getRemovalListener();
    CacheLoader<LedgerHandleKey, LedgerHandle> loader = getLoader();
    _cache = Caffeine.newBuilder()
                     .expireAfterAccess(config.getExpireAfterAccess(), config.getExpireAfterAccessTimeUnit())
                     .removalListener(removalListener)
                     .build(loader);
  }

  @Override
  public void close() throws IOException {
    _cache.invalidateAll();
  }

  @Override
  public BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    try (Scope walWrite = TracerUtil.trace("wal write")) {
      LedgerHandleKey ledgerHandleKey = LedgerHandleKey.builder()
                                                       .volumeId(volumeId)
                                                       .blockId(blockId)
                                                       .build();
      LedgerHandle ledgerHandle = getCurrentLedgerHandle(ledgerHandleKey);
      LOGGER.debug("write ledger id {} volume id {} block id {} generation {} position {} len {}", ledgerHandle.getId(),
          volumeId, blockId, generation, position, len);
      try (Scope performWrite = TracerUtil.trace("perform write async")) {
        CompletableFuture<Long> completableFuture = ledgerHandle.appendAsync(
            createEntry(generation, position, bytes, offset, len));
        return () -> {
          try {
            completableFuture.get();
          } catch (InterruptedException e1) {
            LOGGER.error("Unknown error", e1);
            throw new IOException(e1);
          } catch (ExecutionException e2) {
            Throwable cause = e2.getCause();
            LOGGER.error("Unknown error", cause);
            throw new IOException(cause);
          }
        };
      }
    }
  }

  @Override
  public void release(long volumeId, long blockId, long generation) throws IOException {
    _cache.invalidate(LedgerHandleKey.builder()
                                     .volumeId(volumeId)
                                     .blockId(blockId)
                                     .build());
    List<Long> ledgerIds = getLedgerIdsForBlock(volumeId, blockId);
    List<Long> deleteIds = new ArrayList<>();
    for (Long ledgerId : ledgerIds) {
      try (LedgerHandle ledgerHandle = openLedgerHandle(volumeId, blockId, ledgerId)) {
        long maxGenerationFromLedger = getMaxGenerationFromLedger(ledgerHandle);
        if (maxGenerationFromLedger <= generation) {
          LOGGER.info("Deleting ledger id {} max generation {} on disk generation {}", ledgerId,
              maxGenerationFromLedger, generation);
          deleteIds.add(ledgerId);
        }
      } catch (BKException | InterruptedException e) {
        LOGGER.error("Unknown error", e);
        throw new IOException(e);
      }
    }
    for (Long ledgerId : deleteIds) {
      try {
        LOGGER.info("Removing ledger id {}", ledgerId);
        removeLedgerId(volumeId, blockId, ledgerId);
        _bookKeeper.deleteLedger(ledgerId);
      } catch (BKException | InterruptedException e) {
        LOGGER.error("Unknown error", e);
        throw new IOException(e);
      }
    }
  }

  @Override
  public long recover(FileChannel channel, long volumeId, long blockId, long onDiskGeneration) throws IOException {
    long lastGeneration = onDiskGeneration;
    List<Long> ledgerIds = getLedgerIdsForBlock(volumeId, blockId);
    for (Long ledgerId : ledgerIds) {
      try (LedgerHandle ledgerHandle = openLedgerHandle(volumeId, blockId, ledgerId)) {
        long maxGenerationFromLedger = getMaxGenerationFromLedger(ledgerHandle);
        if (maxGenerationFromLedger < onDiskGeneration) {
          LOGGER.info("Skipping old ledger id {} with max generation {} on disk generation {}", ledgerId,
              maxGenerationFromLedger, onDiskGeneration);
          continue;
        }
        long last = ledgerHandle.readLastAddConfirmed();
        LedgerEntries entries = ledgerHandle.read(0, last);
        for (LedgerEntry entry : entries) {
          lastGeneration = write(entry, channel, onDiskGeneration);
        }
      } catch (org.apache.bookkeeper.client.api.BKException e) {
        if (e.getCode() == Code.NoSuchLedgerExistsException) {
          removeLedgerId(volumeId, blockId, ledgerId);
        } else {
          LOGGER.error("Unknown error", e);
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Unknown error", e);
        throw new IOException(e);
      }
    }
    return lastGeneration;
  }

  private LedgerHandle openLedgerHandle(long volumeId, long blockId, Long ledgerId)
      throws BKException, InterruptedException {
    return _bookKeeper.openLedger(ledgerId, _digestType, getPasswd(volumeId, blockId));
  }

  private long write(LedgerEntry entry, FileChannel channel, long onDiskGeneration) throws IOException {
    ByteBuffer bb = entry.getEntryNioBuffer();
    long generation = bb.getLong();
    if (generation <= onDiskGeneration) {
      return onDiskGeneration;
    }
    long position = bb.getLong();
    while (bb.remaining() > 0) {
      position += channel.write(bb, position);
    }
    return generation;
  }

  private List<Long> getLedgerIdsForBlock(long volumeId, long blockId) throws IOException {
    List<Long> result = new ArrayList<>();
    try {
      String path = new StringBuilder().append(SEP)
                                       .append(LEDGER_STORE)
                                       .append(SEP)
                                       .append(volumeId)
                                       .append(SEP)
                                       .append(blockId)
                                       .toString();
      if (_curatorFramework.checkExists()
                           .forPath(path) == null) {
        return result;
      }
      List<String> ledgerIds = _curatorFramework.getChildren()
                                                .forPath(path);
      for (String ledgerIdStr : ledgerIds) {
        long ledgerId = Long.parseLong(ledgerIdStr);
        result.add(ledgerId);
      }
      return result;
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException(e);
    }
  }

  private void removeLedgerId(long volumeId, long blockId, long ledgerId) throws IOException {
    String path = getZkPath(volumeId, blockId, ledgerId);
    try {
      _curatorFramework.delete()
                       .forPath(path);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException(e);
    }
  }

  private void storeLedgerId(long volumeId, long blockId, long ledgerId) throws IOException {
    String path = getZkPath(volumeId, blockId, ledgerId);
    try {
      _curatorFramework.create()
                       .creatingParentsIfNeeded()
                       .forPath(path);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException(e);
    }
  }

  private String getZkPath(long volumeId, long blockId, long ledgerId) {
    return new StringBuilder().append(SEP)
                              .append(LEDGER_STORE)
                              .append(SEP)
                              .append(volumeId)
                              .append(SEP)
                              .append(blockId)
                              .append(SEP)
                              .append(ledgerId)
                              .toString();
  }

  private byte[] getPasswd(long volumeId, long blockId) {
    ByteBuffer buffer = ByteBuffer.allocate(16)
                                  .putLong(volumeId)
                                  .putLong(blockId);
    buffer.flip();
    return buffer.array();
  }

  public static long getMaxGenerationFromLedger(LedgerHandle ledgerHandle) throws IOException {
    try {
      long id = ledgerHandle.readLastAddConfirmed();
      return getGeneration(getEntry(ledgerHandle, id));
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException(e);
    }
  }

  public static long getMinGenerationFromLedger(LedgerHandle ledgerHandle) throws IOException {
    try {
      long id = 0;
      return getGeneration(getEntry(ledgerHandle, id));
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException(e);
    }
  }

  private static LedgerEntry getEntry(LedgerHandle ledgerHandle, long id)
      throws org.apache.bookkeeper.client.api.BKException, InterruptedException {
    LedgerEntries entries = ledgerHandle.read(id, id);
    for (LedgerEntry entry : entries) {
      return entry;
    }
    return null;
  }

  private static long getGeneration(LedgerEntry entry) {
    if (entry == null) {
      return 0;
    }
    return entry.getEntryNioBuffer()
                .getLong();
  }

  private byte[] getPasswd(LedgerHandleKey key) {
    return getPasswd(key.getVolumeId(), key.getBlockId());
  }

  private ByteBuffer createEntry(long generation, long position, byte[] bytes, int offset, int len) {
    ByteBuffer buffer = ByteBuffer.allocate(len + 16)
                                  .putLong(generation)
                                  .putLong(position)
                                  .put(bytes, offset, len);
    buffer.flip();
    return buffer;
  }

  private LedgerHandle getCurrentLedgerHandle(LedgerHandleKey ledgerHandleKey) {
    try (Scope getLedger = TracerUtil.trace("get ledger")) {
      LedgerHandle ledgerHandle = _cache.get(ledgerHandleKey);
      if (ledgerHandle.isClosed()) {
        closeCurrentLedgerHandle(ledgerHandleKey);
        ledgerHandle = _cache.get(ledgerHandleKey);
      }
      return ledgerHandle;
    }
  }

  private void closeCurrentLedgerHandle(LedgerHandleKey ledgerHandleKey) {
    _cache.invalidate(ledgerHandleKey);
  }

  private Closeable toCloseable(@Nullable LedgerHandle value) {
    return () -> {
      try {
        value.close();
      } catch (BKException | InterruptedException e) {
        throw new IOException(e);
      }
    };
  }

  private RemovalListener<LedgerHandleKey, LedgerHandle> getRemovalListener() {
    return new RemovalListener<LedgerHandleKey, LedgerHandle>() {
      @Override
      public void onRemoval(LedgerHandleKey key, LedgerHandle value, RemovalCause cause) {
        IOUtils.close(LOGGER, toCloseable(value));
      }
    };
  }

  private CacheLoader<LedgerHandleKey, LedgerHandle> getLoader() {
    return key -> {
      LedgerHandle ledger = _bookKeeper.createLedger(_ensSize, _writeQuorumSize, _ackQuorumSize, _digestType,
          getPasswd(key), getMetadata(key));
      storeLedgerId(key.getVolumeId(), key.getBlockId(), ledger.getId());
      return ledger;
    };
  }

  private Map<String, byte[]> getMetadata(LedgerHandleKey key) {
    byte[] volumeId = Long.toString(key.getVolumeId())
                          .getBytes();
    byte[] blockId = Long.toString(key.getBlockId())
                         .getBytes();
    return ImmutableMap.of(VOLUME_ID, volumeId, BLOCK_ID, blockId);
  }
}
