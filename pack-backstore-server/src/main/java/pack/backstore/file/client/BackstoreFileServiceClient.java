package pack.backstore.file.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.backstore.file.thrift.generated.BackstoreFileService;
import pack.iscsi.io.IOUtils;

public class BackstoreFileServiceClient extends BackstoreFileService.Client
    implements BackstoreFileService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BackstoreFileServiceClient.class);

  private final TSocket _transport;

  public BackstoreFileServiceClient(TProtocol protocol, TSocket transport) {
    super(protocol);
    _transport = transport;
  }

  public static BackstoreFileServiceClient create(BackstoreFileServiceClientConfig config) throws IOException {
    try {
      String hostname = config.getHostname();
      int port = config.getPort();
      LOGGER.debug("Creating a new client host {} port {}", hostname, port);
      TSocket socketTransport = getTSocket(config);
      TFramedTransport framedTransport = new TFramedTransport(socketTransport, 1024 * 1024);
      TProtocol protocol = new TCompactProtocol(framedTransport);
      return new BackstoreFileServiceClient(protocol, socketTransport);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private static TSocket getTSocket(BackstoreFileServiceClientConfig config) throws TTransportException {
    String hostname = config.getHostname();
    int port = config.getPort();
    TSocket socket = new TSocket(hostname, port);
    socket.setTimeout(config.getClientTimeout());
    socket.open();
    return socket;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _transport);
  }

}
