package pack.iscsi.wal.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.block.BlockKey;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockJournalResult;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.wal.remote.curator.CuratorUtil;
import pack.iscsi.wal.remote.curator.PackWalHostEntry;
import pack.iscsi.wal.remote.generated.FetchJournalEntriesRequest;
import pack.iscsi.wal.remote.generated.FetchJournalEntriesResponse;
import pack.iscsi.wal.remote.generated.JournalEntry;
import pack.iscsi.wal.remote.generated.JournalRange;
import pack.iscsi.wal.remote.generated.JournalRangeRequest;
import pack.iscsi.wal.remote.generated.JournalRangeResponse;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;
import pack.iscsi.wal.remote.thrift.PackWalServiceClient;
import pack.iscsi.wal.remote.thrift.PackWalServiceClientImpl;

public class RemoteWriteAheadLogClient implements BlockWriteAheadLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteAheadLogClient.class);

  @Value
  @Builder(toBuilder = true)
  public static class RemoteWriteAheadLogClientConfig {
    @Builder.Default
    String hostname = "localhost";

    @Builder.Default
    int port = 8312;

    @Builder.Default
    long timeout = TimeUnit.SECONDS.toMillis(60);

    CuratorFramework curatorFramework;

    String zkPrefix;
  }

  private final String _hostname;
  private final int _port;
  private final int _timeout;
  private final CuratorFramework _curatorFramework;
  private final LoadingCache<BlockKey, PackWalServiceClientImpl> _cache;
  private final String _zkPrefix;

  public RemoteWriteAheadLogClient(RemoteWriteAheadLogClientConfig config) {
    _zkPrefix = config.getZkPrefix();
    _curatorFramework = config.getCuratorFramework();
    _hostname = config.getHostname();
    _port = config.getPort();
    _timeout = (int) config.getTimeout();
    RemovalListener<BlockKey, PackWalServiceClientImpl> removalListener = (key, value, cause) -> {
      if (value != null) {
        value.closeTransport();
      }
    };
    CacheLoader<BlockKey, PackWalServiceClientImpl> loader = key -> newClient();
    _cache = Caffeine.newBuilder()
                     .removalListener(removalListener)
                     .expireAfterAccess(_timeout - 100, TimeUnit.MILLISECONDS)
                     .build(loader);
  }

  private PackWalHostEntry newPackWalHostEntry() throws IOException {
    if (_curatorFramework == null) {
      return PackWalHostEntry.builder()
                             .hostname(_hostname)
                             .port(_port)
                             .build();
    }
    List<PackWalHostEntry> entries;
    try {
      entries = CuratorUtil.getRegisteredServers(_curatorFramework, _zkPrefix);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return pickOne(entries);
  }

  private PackWalHostEntry pickOne(List<PackWalHostEntry> entries) {
    if (entries.size() == 0) {
      return null;
    } else if (entries.size() == 1) {
      return entries.get(0);
    } else {
      Random random = new Random();
      return entries.get(random.nextInt(entries.size()));
    }
  }

  @Override
  public BlockJournalResult write(long volumeId, long blockId, long generation, long position, byte[] bytes, int offset,
      int len) throws IOException {
    try (PackWalServiceClient client = getClient(volumeId, blockId)) {
      synchronized (client) {
        WriteRequest writeRequest = new WriteRequest(volumeId, blockId, generation, position,
            ByteBuffer.wrap(bytes, offset, len));
        client.write(writeRequest);
        return () -> {
        };
      }
    } catch (Exception e) {
      handleError(volumeId, e);
      throw new IOException(e);
    }
  }

  @Override
  public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {
    try (PackWalServiceClient client = getClient(volumeId, blockId)) {
      synchronized (client) {
        ReleaseRequest releaseRequest = new ReleaseRequest(volumeId, blockId, generation);
        client.releaseJournals(releaseRequest);
      }
    } catch (Exception e) {
      handleError(volumeId, e);
      throw new IOException(e);
    }
  }

  @Override
  public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
      boolean closeExistingWriter) throws IOException {
    try (PackWalServiceClient client = getClient(volumeId, blockId)) {
      synchronized (client) {
        JournalRangeRequest journalRangeRequest = new JournalRangeRequest(volumeId, blockId, onDiskGeneration,
            closeExistingWriter);
        List<BlockJournalRange> results = new ArrayList<>();
        JournalRangeResponse journalRanges = client.journalRanges(journalRangeRequest);
        for (JournalRange journalRange : journalRanges.getJournalRangeList()) {
          results.add(toBlockJournalRange(journalRange));
        }
        return results;
      }
    } catch (Exception e) {
      handleError(volumeId, e);
      throw new IOException(e);
    }
  }

  @Override
  public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
      throws IOException {
    long volumeId = range.getVolumeId();
    long blockId = range.getBlockId();
    String uuid = range.getUuid();
    try (PackWalServiceClient client = getClient(volumeId, blockId)) {
      synchronized (client) {
        while (true) {
          FetchJournalEntriesRequest fetchJournalEntriesRequest = new FetchJournalEntriesRequest(volumeId, blockId,
              uuid, range.getMaxGeneration(), range.getMinGeneration(), onDiskGeneration);
          FetchJournalEntriesResponse fetchJournalEntriesResponse = client.fetchJournalEntries(
              fetchJournalEntriesRequest);
          onDiskGeneration = applyJournalEntries(writer, onDiskGeneration, fetchJournalEntriesResponse);
          if (fetchJournalEntriesResponse.isJournalExhausted()) {
            return onDiskGeneration;
          }
        }
      }
    } catch (Exception e) {
      handleError(volumeId, e);
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    _cache.invalidateAll();
  }

  private long applyJournalEntries(BlockRecoveryWriter writer, long onDiskGeneration,
      FetchJournalEntriesResponse fetchJournalEntriesResponse) throws IOException {
    for (JournalEntry journalEntry : fetchJournalEntriesResponse.getEntries()) {
      if (!writer.writeEntry(journalEntry.getGeneration(), journalEntry.getPosition(), journalEntry.getData())) {
        return journalEntry.getGeneration();
      }
      onDiskGeneration = journalEntry.getGeneration();
    }
    return onDiskGeneration;
  }

  private PackWalServiceClient getClient(long volumeId, long blockId) throws IOException {
    return _cache.get(BlockKey.builder()
                              .blockId(blockId)
                              .volumeId(volumeId)
                              .build());
  }

  private void handleError(long volumeId, Exception e) {
    LOGGER.error("Unknown error", e);
    _cache.invalidate(volumeId);
  }

  private PackWalServiceClientImpl newClient() throws IOException {
    try {
      PackWalHostEntry entry = newPackWalHostEntry();
      TSocket transport = new TSocket(entry.getHostname(), entry.getPort());
      transport.setTimeout(_timeout);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      return new PackWalServiceClientImpl(entry.getHostname(), entry.getPort(), protocol, transport.getSocket()
                                                                                                   .toString());
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private BlockJournalRange toBlockJournalRange(JournalRange journalRange) {
    return BlockJournalRange.builder()
                            .blockId(journalRange.getBlockId())
                            .maxGeneration(journalRange.getMaxGeneration())
                            .minGeneration(journalRange.getMinGeneration())
                            .uuid(journalRange.getUuid())
                            .volumeId(journalRange.getVolumeId())
                            .build();
  }
}
