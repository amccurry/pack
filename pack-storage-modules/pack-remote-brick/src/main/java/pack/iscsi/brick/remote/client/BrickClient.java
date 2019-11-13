package pack.iscsi.brick.remote.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.iscsi.brick.remote.generated.PackBrickService;
import pack.iscsi.brick.remote.thrift.TracerTProtocol;
import pack.iscsi.brick.remote.thrift.TracerTransport;
import pack.iscsi.io.IOUtils;
import pack.util.tracer.TracerUtil;

public class BrickClient extends PackBrickService.Client implements PackBrickService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrickClient.class);

  private static final String AUTH_CONF = "auth-conf";
  private static final String TRUE = "true";
  private static final String GSSAPI = "GSSAPI";

  private final TSocket _transport;

  public BrickClient(TProtocol protocol, TSocket transport) {
    super(protocol);
    _transport = transport;
  }

  public static BrickClient create(BrickClientConfig config) throws IOException {
    try (Scope scope = TracerUtil.trace(BrickClient.class, "create")) {
      String hostname = config.getHostname();
      int port = config.getPort();
      LOGGER.debug("Creating a new client host {} port {}", hostname, port);
      TSocket socketTransport = getTSocket(config);
      TFramedTransport framedTransport = new TFramedTransport(socketTransport, 1024 * 1024);
      TTransport transport;
      if (config.isKerberosEnabled()) {
        String mechanism = GSSAPI;
        String authorizationId = null;
        String protocol = config.getKerberosProtocol();
        String serverName = config.getKerberosServerName();
        Map<String, String> saslProperties = new HashMap<String, String>();
        saslProperties.put(Sasl.QOP, TRUE);
        saslProperties.put(Sasl.QOP, AUTH_CONF);
        CallbackHandler cbh = null;
        SaslClient client = Sasl.createSaslClient(new String[] { mechanism }, authorizationId, protocol, serverName,
            saslProperties, cbh);
        transport = new TSaslClientTransport(client, framedTransport);
        transport.open();
      } else {
        transport = framedTransport;
      }
      TProtocol protocol = wrap(new TCompactProtocol(wrap(transport)));
      return new BrickClient(protocol, socketTransport);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private static TSocket getTSocket(BrickClientConfig config) throws TTransportException {
    String hostname = config.getHostname();
    int port = config.getPort();
    if (config.isSslEnabled()) {
      TSSLTransportParameters params = new TSSLTransportParameters();
      params.requireClientAuth(config.isSslClientAuthEnabled());
      params.setKeyStore(config.getSslKeyStore(), config.getSslKeyPass());
      params.setTrustStore(config.getSslTrustStore(), config.getSslTrustPass());
      return TSSLTransportFactory.getClientSocket(hostname, port, config.getClientTimeout(), params);
    } else {
      TSocket socket = new TSocket(hostname, port);
      socket.setTimeout(config.getClientTimeout());
      socket.open();
      return socket;
    }
  }

  private static TProtocol wrap(TProtocol protocol) {
    if (TracerUtil.isEnabled()) {
      return new TracerTProtocol(protocol);
    }
    return protocol;
  }

  private static TTransport wrap(TTransport transport) {
    if (TracerUtil.isEnabled()) {
      return new TracerTransport(transport);
    }
    return transport;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _transport);
  }

}
