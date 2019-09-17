package pack.iscsi.wal.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.iscsi.wal.remote.generated.PackException;
import pack.iscsi.wal.remote.generated.PackWalService;
import pack.iscsi.wal.remote.generated.PackWalService.Client;
import pack.iscsi.wal.remote.generated.ReleaseRequest;
import pack.iscsi.wal.remote.generated.WriteRequest;

public class RemoteWriteAheadLogClient implements BlockWriteAheadLog {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteAheadLogClient.class);

  public static void main(String[] args) throws Exception {

    RemoteWriteAheadLogClientConfig config = RemoteWriteAheadLogClientConfig.builder()
                                                                            .hostname("localhost")
                                                                            .port(8312)
                                                                            .build();

    try (RemoteWriteAheadLogClient client = new RemoteWriteAheadLogClient(config)) {
      Random random = new Random();
      byte[] buffer = new byte[8192];
      long volumeId = random.nextInt(10);
      long blockId = random.nextInt(10);
      long generation = 0;

      long bytesSent = 0;
      long totalTime = 0;
      long start = System.nanoTime();
      long count = 0;
      while (true) {
        if (start + 5_000_000_000L < System.nanoTime()) {
          double seconds = totalTime / 1_000_000_000.0;
          double byteRate = bytesSent / 1024 / 1024 / seconds;
          double latency = seconds / count;
          System.out.printf("%.2f MB/s @ avg lat %.4f ms%n", byteRate, latency);
          client.release(volumeId, blockId, generation);
          start = System.nanoTime();
          totalTime = 0;
          bytesSent = 0;
          count = 0;
        }
        generation++;
        random.nextBytes(buffer);
        long position = random.nextInt(Integer.MAX_VALUE);
        bytesSent += buffer.length;
        long s = System.nanoTime();
        client.write(volumeId, blockId, generation, position, buffer);
        long e = System.nanoTime();
        totalTime += (e - s);
        count++;
      }
    }
  }

  @Value
  @Builder(toBuilder = true)
  public static class RemoteWriteAheadLogClientConfig {
    String hostname;
    int port;
  }

  private final String _hostname;
  private final int _port;
  private final BlockingQueue<PackWalService.Client> _clients = new ArrayBlockingQueue<>(10);

  public RemoteWriteAheadLogClient(RemoteWriteAheadLogClientConfig config) {
    _hostname = config.getHostname();
    _port = config.getPort();
  }

  @Override
  public BlockWriteAheadLogResult write(long volumeId, long blockId, long generation, long position, byte[] bytes,
      int offset, int len) throws IOException {
    PackWalService.Client client = getClient();
    try {
      WriteRequest writeRequest = new WriteRequest(volumeId, blockId, generation, position,
          ByteBuffer.wrap(bytes, offset, len));
      client.write(writeRequest);
    } catch (PackException e) {
      LOGGER.error("Unknown error", e);
      closeClient(client);
      throw new IOException(e);
    } catch (TException e) {
      LOGGER.error("Unknown error", e);
      closeClient(client);
      throw new IOException(e);
    } finally {
      release(client);
    }
    return () -> {
    };
  }

  @Override
  public void release(long volumeId, long blockId, long generation) throws IOException {
    Client client = getClient();
    try {
      ReleaseRequest releaseRequest = new ReleaseRequest(volumeId, blockId, generation);
      client.release(releaseRequest);
    } catch (PackException e) {
      LOGGER.error("Unknown error", e);
      closeClient(client);
      throw new IOException(e);
    } catch (TException e) {
      LOGGER.error("Unknown error", e);
      closeClient(client);
      throw new IOException(e);
    } finally {
      release(client);
    }
  }

  @Override
  public long recover(RandomAccessIO randomAccessIO, long volumeId, long blockId, long onDiskGeneration)
      throws IOException {
    throw new RuntimeException("not impl");
  }

  @Override
  public void close() throws IOException {
    for (PackWalService.Client client : _clients) {
      closeClient(client);
    }
  }

  private PackWalService.Client getClient() throws IOException {
    PackWalService.Client client = _clients.poll();
    if (client == null) {
      return newClient();
    }
    return client;
  }

  private void release(PackWalService.Client client) {
    if (!_clients.offer(client)) {
      closeClient(client);
    }
  }

  private void closeClient(PackWalService.Client client) {
    client.getInputProtocol()
          .getTransport()
          .close();
  }

  private PackWalService.Client newClient() throws IOException {
    try {
      TSocket transport = new TSocket(_hostname, _port);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      return new PackWalService.Client(protocol);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }
}
