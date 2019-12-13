package pack.thrift.common;

import java.io.IOException;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientFactory.class);

  public static <T> T create(ClientConfig<T> config) throws IOException {
    try {
      String hostname = config.getHostname();
      int port = config.getPort();
      LOGGER.debug("Creating a new client host {} port {}", hostname, port);
      TSocket socketTransport = getTSocket(config);
      TFramedTransport framedTransport = new TFramedTransport(socketTransport, config.getMaxFrameLength());
      TProtocol protocol = new TCompactProtocol(framedTransport);
      return config.createClient(protocol, socketTransport);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private static <T> TSocket getTSocket(ClientConfig<T> config) throws TTransportException {
    String hostname = config.getHostname();
    int port = config.getPort();
    TSocket socket = new TSocket(hostname, port);
    socket.setTimeout(config.getClientTimeout());
    socket.open();
    return socket;
  }

}
