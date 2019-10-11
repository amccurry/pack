package pack.iscsi.wal.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import lombok.Builder;
import lombok.Value;
import pack.iscsi.concurrent.ConcurrentUtils;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.wal.BlockJournalRange;
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
import pack.iscsi.wal.remote.generated.PackException;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;
import pack.iscsi.wal.remote.thrift.PackWalServiceClient;
import pack.iscsi.wal.remote.thrift.PackWalServiceClientImpl;
import pack.util.tracer.TracerUtil;

public class RemoteWALClient implements BlockWriteAheadLog {

  private static final String WAL = "wal";
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWALClient.class);

  @Value
  @Builder(toBuilder = true)
  public static class RemoteWALClientConfig {
    @Builder.Default
    String hostname = "localhost";

    @Builder.Default
    int port = 8312;

    @Builder.Default
    long timeout = TimeUnit.SECONDS.toMillis(60);

    CuratorFramework curatorFramework;

    String zkPrefix;

    @Builder.Default
    int walExecutorThreadCount = 5;

  }

  private final String _hostname;
  private final int _port;
  private final int _timeout;
  private final CuratorFramework _curatorFramework;
  private final String _zkPrefix;
  private final BlockingQueue<PackWalServiceClientImpl> _clients = new ArrayBlockingQueue<>(10);
  private final int _retries = 10;
  private final ExecutorService _walExecutor;

  public RemoteWALClient(RemoteWALClientConfig config) {
    _walExecutor = ConcurrentUtils.executor(WAL, config.getWalExecutorThreadCount());
    _zkPrefix = config.getZkPrefix();
    _curatorFramework = config.getCuratorFramework();
    _hostname = config.getHostname();
    _port = config.getPort();
    _timeout = (int) config.getTimeout();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _walExecutor);
  }

  @Override
  public AsyncCompletableFuture write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    // Needed for thread safety, copy the buffer before returning
    ByteBuffer byteBuffer = (ByteBuffer) ByteBuffer.allocate(len)
                                                   .put(bytes, offset, len)
                                                   .flip();
    return AsyncCompletableFuture.exec(RemoteWALClient.class, "write", _walExecutor, () -> execute(client -> {
      try (Scope scope1 = TracerUtil.trace(RemoteWALClient.class, "wal write")) {
        WriteRequest writeRequest = new WriteRequest(volumeId, blockId, generation, position, byteBuffer);
        try (Scope scope2 = TracerUtil.trace(RemoteWALClient.class, "wal client write")) {
          client.write(writeRequest);
        }
      }
      return null;
    }));
  }

  @Override
  public void releaseJournals(long volumeId, long blockId, long generation) throws IOException {
    execute(client -> {
      ReleaseRequest releaseRequest = new ReleaseRequest(volumeId, blockId, generation);
      client.releaseJournals(releaseRequest);
      return null;
    });
  }

  @Override
  public List<BlockJournalRange> getJournalRanges(long volumeId, long blockId, long onDiskGeneration,
      boolean closeExistingWriter) throws IOException {
    return execute(client -> {
      JournalRangeRequest journalRangeRequest = new JournalRangeRequest(volumeId, blockId, onDiskGeneration,
          closeExistingWriter);
      List<BlockJournalRange> results = new ArrayList<>();
      JournalRangeResponse journalRanges = client.journalRanges(journalRangeRequest);
      for (JournalRange journalRange : journalRanges.getJournalRangeList()) {
        results.add(toBlockJournalRange(journalRange));
      }
      return results;
    });
  }

  @Override
  public long recoverFromJournal(BlockRecoveryWriter writer, BlockJournalRange range, long onDiskGeneration)
      throws IOException {
    long volumeId = range.getVolumeId();
    long blockId = range.getBlockId();
    String uuid = range.getUuid();
    return execute(client -> {
      long onDiskGen = onDiskGeneration;
      while (true) {
        FetchJournalEntriesRequest fetchJournalEntriesRequest = new FetchJournalEntriesRequest(volumeId, blockId, uuid,
            range.getMaxGeneration(), range.getMinGeneration(), onDiskGen);
        FetchJournalEntriesResponse fetchJournalEntriesResponse = client.fetchJournalEntries(
            fetchJournalEntriesRequest);
        onDiskGen = applyJournalEntries(writer, onDiskGen, fetchJournalEntriesResponse, blockId);
        if (fetchJournalEntriesResponse.isJournalExhausted()) {
          return onDiskGen;
        }
      }
    });
  }

  private long applyJournalEntries(BlockRecoveryWriter writer, long onDiskGeneration,
      FetchJournalEntriesResponse fetchJournalEntriesResponse, long blockId) throws IOException {
    for (JournalEntry journalEntry : fetchJournalEntriesResponse.getEntries()) {
      if (journalEntry.getGeneration() <= onDiskGeneration) {
        continue;
      }
      if (!writer.writeEntry(journalEntry.getGeneration(), journalEntry.getPosition(), journalEntry.getData())) {
        return journalEntry.getGeneration();
      }
      onDiskGeneration = journalEntry.getGeneration();
    }
    return onDiskGeneration;
  }

  private IOException handleError(Exception e) {
    if (e instanceof IOException) {
      LOGGER.error("Unknown error", e);
      return (IOException) e;
    } else if (e instanceof PackException) {
      PackException pe = (PackException) e;
      LOGGER.error("Unknown pack error message {} stack {}", pe.getMessage(), pe.getStackTraceStr());
      return new IOException(pe);
    } else {
      LOGGER.error("Unknown error", e);
      return new IOException(e);
    }
  }

  private PackWalServiceClientImpl getClient() throws IOException {
    try (Scope scope = TracerUtil.trace(RemoteWALClient.class, "getClient")) {
      PackWalServiceClientImpl client = _clients.poll();
      if (client != null) {
        if (testClient(client)) {
          return client;
        } else {
          IOUtils.close(LOGGER, client);
        }
      }
      return newClient();
    }
  }

  private boolean testClient(PackWalServiceClientImpl client) {
    try {
      client.ping();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void releaseClient(PackWalServiceClientImpl client) {
    try (Scope scope = TracerUtil.trace(RemoteWALClient.class, "releaseClient")) {
      if (client == null) {
        return;
      }
      if (!_clients.offer(client)) {
        LOGGER.info("Closing client {}", client);
        IOUtils.close(LOGGER, client);
      }
    }
  }

  private PackWalServiceClientImpl newClient() throws IOException {
    try (Scope scope = TracerUtil.trace(RemoteWALClient.class, "newClient")) {
      PackWalHostEntry entry = newPackWalHostEntry();
      LOGGER.debug("Creating a new client host {} port {}", entry.getHostname(), entry.getPort());
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
    PackWalHostEntry entry = pickOne(entries);
    if (entry == null) {
      throw new IOException("No wal servers found.");
    }
    return entry;
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

  static interface Exec<T> {
    T exec(PackWalServiceClient client) throws Exception;
  }

  private <T> T execute(Exec<T> exec) throws IOException {
    Exception lastException = null;
    for (int i = 0; i < _retries; i++) {
      PackWalServiceClientImpl client = null;
      try (Scope scope = TracerUtil.trace(RemoteWALClient.class, "execute")) {
        client = getClient();
        return exec.exec(client);
      } catch (Exception e) {
        LOGGER.error("Unknown error", e);
        IOUtils.close(LOGGER, client);
        client = null;
        lastException = e;
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        } catch (InterruptedException ex) {
          LOGGER.error("Unknown error", ex);
          throw handleError(lastException);
        }
      } finally {
        releaseClient(client);
      }
    }
    throw handleError(lastException);
  }

}
