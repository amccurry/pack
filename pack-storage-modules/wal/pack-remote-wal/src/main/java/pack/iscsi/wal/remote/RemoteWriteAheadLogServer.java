package pack.iscsi.wal.remote;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;
import pack.iscsi.wal.remote.generated.MaxGenerationRequest;
import pack.iscsi.wal.remote.generated.MaxGenerationResponse;
import pack.iscsi.wal.remote.generated.PackException;
import pack.iscsi.wal.remote.generated.PackWalService;
import pack.iscsi.wal.remote.generated.PackWalService.Processor;
import pack.iscsi.wal.remote.generated.RecoverRequest;
import pack.iscsi.wal.remote.generated.RecoverResponse;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;

public class RemoteWriteAheadLogServer implements Closeable, PackWalService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteAheadLogServer.class);

  @Value
  @Builder(toBuilder = true)
  public static class RemoteWriteAheadLogServerConfig {

    @Builder.Default
    String address = "0.0.0.0";

    @Builder.Default
    int port = 8312;

    @Builder.Default
    int clientTimeout = (int) TimeUnit.SECONDS.toMillis(10);

    File walLogDir;

  }

  public static void main(String[] args) throws Exception {
    File walLogDir = new File("./wal");
    IOUtils.rmr(walLogDir);
    RemoteWriteAheadLogServerConfig config = RemoteWriteAheadLogServerConfig.builder()
                                                                            .walLogDir(walLogDir)
                                                                            .build();
    try (RemoteWriteAheadLogServer server = new RemoteWriteAheadLogServer(config)) {
      LOGGER.info("Starting server");
      server.start();
    }
  }

  private final TServer _server;
  private final LocalBlockWriteAheadLog _log;

  public RemoteWriteAheadLogServer(RemoteWriteAheadLogServerConfig config) throws Exception {
    InetSocketAddress bindAddr = new InetSocketAddress(config.getAddress(), config.getPort());
    TServerTransport serverTransport = new TServerSocket(bindAddr, config.getClientTimeout());
    Processor<RemoteWriteAheadLogServer> processor = new PackWalService.Processor<>(this);
    Args args = new TThreadPoolServer.Args(serverTransport).processor(processor)
                                                           .protocolFactory(new TBinaryProtocol.Factory());
    _server = new TThreadPoolServer(args);
    _log = new LocalBlockWriteAheadLog(LocalBlockWriteAheadLogConfig.builder()
                                                                    .walLogDir(config.getWalLogDir())
                                                                    .build());
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, () -> _server.stop());
  }

  public void start() {
    _server.serve();
  }

  @Override
  public void write(WriteRequest writeRequest) throws PackException, TException {
    long volumeId = writeRequest.getVolumeId();
    long blockId = writeRequest.getBlockId();
    long generation = writeRequest.getGeneration();
    long position = writeRequest.getPosition();
    byte[] data = writeRequest.getData();
    try {
      BlockWriteAheadLogResult result = _log.write(volumeId, blockId, generation, position, data);
      result.get();
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new PackException(e.getMessage());
    }
  }

  @Override
  public void release(ReleaseRequest releaseRequest) throws PackException, TException {
    long volumeId = releaseRequest.getVolumeId();
    long blockId = releaseRequest.getBlockId();
    long generation = releaseRequest.getGeneration();
    try {
      LOGGER.info("release volumeId {} blockId {} generation {}", volumeId, blockId, generation);
      _log.release(volumeId, blockId, generation);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new PackException(e.getMessage());
    }
  }

  @Override
  public MaxGenerationResponse maxGeneration(MaxGenerationRequest maxGenerationRequest)
      throws PackException, TException {
    long volumeId = maxGenerationRequest.getVolumeId();
    long blockId = maxGenerationRequest.getBlockId();
    try {
      long generation = _log.getMaxGeneration(volumeId, blockId);
      return new MaxGenerationResponse(generation);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new PackException(e.getMessage());
    }
  }

  @Override
  public RecoverResponse recover(RecoverRequest recoverRequest) throws PackException, TException {
    throw new RuntimeException("not impl");
  }
}
