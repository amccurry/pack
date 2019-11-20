package pack.volume.client;

import java.io.Closeable;
import java.io.IOException;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.iscsi.io.IOUtils;
import pack.util.tracer.TracerUtil;
import pack.volume.thrift.generated.PackVolumeService;

public class VolumeClient extends PackVolumeService.Client implements PackVolumeService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeClient.class);

  private final TSocket _transport;

  public VolumeClient(TProtocol protocol, TSocket transport) {
    super(protocol);
    _transport = transport;
  }

  public static VolumeClient create(VolumeClientConfig config) throws IOException {
    try (Scope scope = TracerUtil.trace(VolumeClient.class, "create")) {
      String hostname = config.getHostname();
      int port = config.getPort();
      LOGGER.debug("Creating a new client host {} port {}", hostname, port);
      TSocket socketTransport = getTSocket(config);
      TFramedTransport framedTransport = new TFramedTransport(socketTransport, 1024 * 1024);
      TProtocol protocol = new TCompactProtocol(framedTransport);
      return new VolumeClient(protocol, socketTransport);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private static TSocket getTSocket(VolumeClientConfig config) throws TTransportException {
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
