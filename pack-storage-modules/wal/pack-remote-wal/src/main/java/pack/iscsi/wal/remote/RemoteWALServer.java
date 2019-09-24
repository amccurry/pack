package pack.iscsi.wal.remote;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockJournalResult;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;
import pack.iscsi.wal.remote.curator.CuratorUtil;
import pack.iscsi.wal.remote.generated.FetchJournalEntriesRequest;
import pack.iscsi.wal.remote.generated.FetchJournalEntriesResponse;
import pack.iscsi.wal.remote.generated.JournalEntry;
import pack.iscsi.wal.remote.generated.JournalRange;
import pack.iscsi.wal.remote.generated.JournalRangeRequest;
import pack.iscsi.wal.remote.generated.JournalRangeResponse;
import pack.iscsi.wal.remote.generated.PackException;
import pack.iscsi.wal.remote.generated.PackWalService;
import pack.iscsi.wal.remote.generated.PackWalService.Processor;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;

public class RemoteWALServer implements Closeable, PackWalService.Iface {

  private static final String SERVER = "server";
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWALServer.class);

  @Value
  @Builder(toBuilder = true)
  public static class RemoteWriteAheadLogServerConfig {

    @Builder.Default
    String address = "0.0.0.0";

    @Builder.Default
    int port = 8312;

    @Builder.Default
    int clientTimeout = (int) TimeUnit.SECONDS.toMillis(10);

    @Builder.Default
    int maxEntryPayload = 1024 * 1024;

    File walLogDir;

    CuratorFramework curatorFramework;

    String zkPrefix;

  }

  private final TServer _server;
  private final LocalBlockWriteAheadLog _log;
  private final int _maxEntryPayload;
  private final CuratorFramework _curatorFramework;
  private final TServerSocket _serverTransport;
  private final String _zkPrefix;

  public RemoteWALServer(RemoteWriteAheadLogServerConfig config) throws Exception {
    _zkPrefix = config.getZkPrefix();
    _maxEntryPayload = config.getMaxEntryPayload();
    _curatorFramework = config.getCuratorFramework();
    InetSocketAddress bindAddr = new InetSocketAddress(config.getAddress(), config.getPort());
    _serverTransport = new TServerSocket(bindAddr, config.getClientTimeout());
    Processor<RemoteWALServer> processor = new PackWalService.Processor<>(this);
    Args args = new TThreadPoolServer.Args(_serverTransport).processor(handleClosedConnections(processor))
                                                            .protocolFactory(new TBinaryProtocol.Factory())
                                                            .minWorkerThreads(10)
                                                            .maxWorkerThreads(10);
    _server = new TThreadPoolServer(args);
    _log = new LocalBlockWriteAheadLog(LocalBlockWriteAheadLogConfig.builder()
                                                                    .walLogDir(config.getWalLogDir())
                                                                    .build());
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, () -> _server.stop());
  }

  public void start(boolean blocking) throws Exception {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        _server.serve();
      }
    });
    thread.setName(SERVER);
    thread.setDaemon(true);
    thread.start();

    while (!_server.isServing()) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    LOGGER.info("Listening on {} port {}", getBindInetAddress(), getBindPort());
    CuratorUtil.registerServer(_curatorFramework, _zkPrefix, getBindInetAddress(), getBindPort());
    if (blocking) {
      thread.join();
    }
  }

  public InetAddress getBindInetAddress() {
    return _serverTransport.getServerSocket()
                           .getInetAddress();
  }

  public int getBindPort() {
    return _serverTransport.getServerSocket()
                           .getLocalPort();
  }

  public void stop() throws Exception {
    CuratorUtil.removeRegistration(_curatorFramework, _zkPrefix, getBindInetAddress(), getBindPort());
    _server.stop();
  }

  @Override
  public void write(WriteRequest writeRequest) throws PackException, TException {
    long volumeId = writeRequest.getVolumeId();
    long blockId = writeRequest.getBlockId();
    long generation = writeRequest.getGeneration();
    long position = writeRequest.getPosition();
    byte[] data = writeRequest.getData();
    try {
      BlockJournalResult result = _log.write(volumeId, blockId, generation, position, data);
      result.get();
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw newPackException(e);
    }
  }

  private PackException newPackException(Exception e) {
    StringWriter writer = new StringWriter();
    try (PrintWriter pw = new PrintWriter(writer)) {
      e.printStackTrace(pw);
    }
    return new PackException(e.getMessage(), writer.toString());
  }

  @Override
  public void releaseJournals(ReleaseRequest releaseRequest) throws PackException, TException {
    long volumeId = releaseRequest.getVolumeId();
    long blockId = releaseRequest.getBlockId();
    long generation = releaseRequest.getGeneration();
    try {
      LOGGER.info("releaseJournals volumeId {} blockId {} generation {}", volumeId, blockId, generation);
      _log.releaseJournals(volumeId, blockId, generation);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw newPackException(e);
    }
  }

  @Override
  public JournalRangeResponse journalRanges(JournalRangeRequest journalRangeRequest) throws PackException, TException {
    long volumeId = journalRangeRequest.getVolumeId();
    long blockId = journalRangeRequest.getBlockId();
    long onDiskGeneration = journalRangeRequest.getOnDiskGeneration();
    boolean closeExistingWriter = journalRangeRequest.isCloseExistingWriter();
    try {
      LOGGER.info("journalRanges volumeId {} blockId {} onDiskGeneration {} closeExistingWriter{}", volumeId, blockId,
          onDiskGeneration, closeExistingWriter);
      List<BlockJournalRange> journalRanges = _log.getJournalRanges(volumeId, blockId, onDiskGeneration,
          closeExistingWriter);
      List<JournalRange> journalRangeList = new ArrayList<>();
      for (BlockJournalRange blockJournalRange : journalRanges) {
        journalRangeList.add(toJournalRange(blockJournalRange));
      }
      return new JournalRangeResponse(journalRangeList);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw newPackException(e);
    }
  }

  @Override
  public FetchJournalEntriesResponse fetchJournalEntries(FetchJournalEntriesRequest fetchJournalEntriesRequest)
      throws PackException, TException {
    long volumeId = fetchJournalEntriesRequest.getVolumeId();
    long blockId = fetchJournalEntriesRequest.getBlockId();
    String uuid = fetchJournalEntriesRequest.getUuid();
    long maxGeneration = fetchJournalEntriesRequest.getMaxGeneration();
    long minGeneration = fetchJournalEntriesRequest.getMinGeneration();
    long onDiskGeneration = fetchJournalEntriesRequest.getOnDiskGeneration();
    BlockJournalRange range = BlockJournalRange.builder()
                                               .blockId(blockId)
                                               .maxGeneration(maxGeneration)
                                               .minGeneration(minGeneration)
                                               .uuid(uuid)
                                               .volumeId(volumeId)
                                               .build();

    PartialBlockRecoveryWriter writer = new PartialBlockRecoveryWriter(_maxEntryPayload);
    try {
      _log.recoverFromJournal(writer, range, onDiskGeneration);
      return new FetchJournalEntriesResponse(volumeId, blockId, writer.isJournalExhausted(), writer.getEntries());
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw newPackException(e);
    }
  }

  private JournalRange toJournalRange(BlockJournalRange blockJournalRange) {
    return new JournalRange(blockJournalRange.getVolumeId(), blockJournalRange.getBlockId(),
        blockJournalRange.getUuid(), blockJournalRange.getMinGeneration(), blockJournalRange.getMaxGeneration());
  }

  static class PartialBlockRecoveryWriter implements BlockRecoveryWriter {
    private int _size;
    private boolean _journalExhausted = true;
    private List<JournalEntry> _entries = new ArrayList<>();
    private final int _maxEntryPayload;

    PartialBlockRecoveryWriter(int maxEntryPayload) {
      _maxEntryPayload = maxEntryPayload;
    }

    @Override
    public boolean writeEntry(long generation, long position, byte[] buffer, int offset, int length)
        throws IOException {
      _size += length;
      if (isTooLarge(_size)) {
        _journalExhausted = false;
        return false;
      }
      _entries.add(new JournalEntry(generation, position, ByteBuffer.wrap(buffer, offset, length)));
      return true;
    }

    private boolean isTooLarge(int entryPayload) {
      return entryPayload >= _maxEntryPayload;
    }

    boolean isJournalExhausted() {
      return _journalExhausted;
    }

    List<JournalEntry> getEntries() {
      return _entries;
    }
  }

  private TProcessor handleClosedConnections(Processor<RemoteWALServer> processor) {
    return (in, out) -> {
      try {
        return processor.process(in, out);
      } catch (TTransportException e) {
        switch (e.getType()) {
        case TTransportException.END_OF_FILE:
          LOGGER.debug("Client closed connection");
          return false;
        case TTransportException.UNKNOWN:
          LOGGER.debug("Client connection terminated, possible timeout");
          return false;
        default:
          throw e;
        }
      }
    };
  }

  @Override
  public void ping() throws PackException, TException {

  }
}
