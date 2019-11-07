package pack.iscsi.brick.remote;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.brick.remote.generated.DestroyRequest;
import pack.iscsi.brick.remote.generated.DestroyResponse;
import pack.iscsi.brick.remote.generated.ListBricksRequest;
import pack.iscsi.brick.remote.generated.ListBricksResponse;
import pack.iscsi.brick.remote.generated.PackBrickException;
import pack.iscsi.brick.remote.generated.PackBrickService;
import pack.iscsi.brick.remote.generated.PackBrickService.Processor;
import pack.iscsi.brick.remote.generated.WriteRequest;
import pack.iscsi.io.IOUtils;

public abstract class BaseBrickServer implements PackBrickService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseBrickServer.class);

  private static final String AUTH_CONF = "auth-conf";
  private static final String TRUE = "true";
  private static final String GSSAPI = "GSSAPI";
  private static final String SERVER = "server";

  private final TServerTransport _serverTransport;
  private final TServer _server;
  private final boolean _nonBlocking;
  private final int _minThreads;
  private final int _maxThreads;
  protected final File _brickDir;

  public BaseBrickServer(BrickServerConfig config) throws TTransportException {
    _nonBlocking = config.isNonBlockingRpc();
    _minThreads = config.getMinThreads();
    _maxThreads = config.getMaxThreads();
    _brickDir = config.getBrickDir();
    _brickDir.mkdirs();
    InetSocketAddress bindAddr = new InetSocketAddress(config.getAddress(), config.getPort());
    _serverTransport = createServerTransport(config, bindAddr);
    _server = createServer(config);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, () -> _server.stop());
  }

  public void start(boolean blocking) throws Exception {
    Thread thread = new Thread(() -> _server.serve());
    thread.setName(SERVER);
    thread.setDaemon(true);
    thread.start();

    while (!_server.isServing()) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    LOGGER.info("Listening on {} port {}", getBindInetAddress(), getBindPort());
    if (blocking) {
      thread.join();
    }
  }

  @Override
  public void writeNonBlocking(WriteRequest request) throws TException {
    write(request);
  }

  @Override
  public ListBricksResponse listBricks(ListBricksRequest request) throws PackBrickException, TException {
    try {
      List<Long> brickIds = new ArrayList<>();
      File[] files = _brickDir.listFiles();
      for (File file : files) {
        if (file.isFile()) {
          brickIds.add(Long.parseLong(file.getName()));
        }
      }
      return new ListBricksResponse(brickIds);
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  @Override
  public DestroyResponse destroy(DestroyRequest request) throws PackBrickException, TException {
    try {
      closeFileHandle(request.getBrickId());
      File file = getFile(request.getBrickId());
      file.delete();
      return new DestroyResponse();
    } catch (Exception e) {
      throw newPackBrickException(e);
    }
  }

  protected abstract void closeFileHandle(long brickId);

  public InetAddress getBindInetAddress() {
    if (_serverTransport instanceof TServerSocket) {
      return ((TServerSocket) _serverTransport).getServerSocket()
                                               .getInetAddress();
    } else {
      return null;
    }
  }

  public int getBindPort() {
    if (_serverTransport instanceof TServerSocket) {
      return ((TServerSocket) _serverTransport).getServerSocket()
                                               .getLocalPort();
    } else {
      return -1;
    }
  }

  @Override
  public void noop() throws PackBrickException, TException {

  }

  protected PackBrickException newPackBrickException(Exception e) {
    StringWriter writer = new StringWriter();
    try (PrintWriter pw = new PrintWriter(writer)) {
      e.printStackTrace(pw);
    }
    return new PackBrickException(e.getMessage(), writer.toString());
  }

  protected File getFile(long brickId) {
    return new File(_brickDir, Long.toString(brickId));
  }

  private TProcessor handleClosedConnections(Processor<?> processor) {
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

  private TServer createServer(BrickServerConfig config) {
    Processor<BaseBrickServer> processor = new PackBrickService.Processor<>(this);
    TFramedTransport.Factory framedTransportFactory = new TFramedTransport.Factory(1024 * 1024);
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();

    if (_nonBlocking) {
      TNonblockingServerTransport transport = (TNonblockingServerTransport) _serverTransport;
      THsHaServer.Args args = new THsHaServer.Args(transport);
      args.processor(processor)
          .protocolFactory(protocolFactory)
          .transportFactory(framedTransportFactory)
          .minWorkerThreads(_minThreads)
          .maxWorkerThreads(_maxThreads);
      return new THsHaServer(args);
    } else {
      TTransportFactory transportFactory;
      if (config.isKerberosEnabled()) {
        String mechanism = GSSAPI;
        String protocol = config.getKerberosProtocol();
        String serverName = config.getKerberosServerName();
        Map<String, String> saslProperties = new HashMap<String, String>();
        saslProperties.put(Sasl.QOP, TRUE);
        saslProperties.put(Sasl.QOP, AUTH_CONF);
        CallbackHandler cbr = callbacks -> {
        };
        TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
        saslTransportFactory.addServerDefinition(mechanism, protocol, serverName, saslProperties, cbr);
        transportFactory = new TTransportFactory() {
          @Override
          public TTransport getTransport(TTransport trans) {
            return saslTransportFactory.getTransport(framedTransportFactory.getTransport(trans));
          }
        };
      } else {
        transportFactory = framedTransportFactory;
      }

      TThreadPoolServer.Args args = new TThreadPoolServer.Args(_serverTransport);
      args.processor(handleClosedConnections(processor))
          .protocolFactory(protocolFactory)
          .transportFactory(transportFactory)
          .minWorkerThreads(_minThreads)
          .maxWorkerThreads(_maxThreads);
      return new TThreadPoolServer(args);
    }
  }

  private TServerTransport createServerTransport(BrickServerConfig config, InetSocketAddress bindAddr)
      throws TTransportException {
    if (config.isSslEnabled() && config.isNonBlockingRpc()) {
      throw new RuntimeException("SSL and Nonblocking options are not supported together.");
    }
    if (config.isNonBlockingRpc()) {
      return new TNonblockingServerSocket(bindAddr, config.getClientTimeout());
    } else if (config.isSslEnabled()) {
      int port = bindAddr.getPort();
      int clientTimeout = config.getClientTimeout();
      InetAddress ifAddress = bindAddr.getAddress();
      // String protocol = "TLS";
      // String[] cipherSuites = new String[] {
      // "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA",
      // "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384",
      // "TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384",
      // "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
      // "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
      // "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
      // "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
      // "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
      // "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
      // "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA",
      // "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
      // "TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384" };
      TSSLTransportParameters params = new TSSLTransportParameters(config.getSslProtocol(),
          config.getSslCipherSuites());
      params.requireClientAuth(config.isSslClientAuthEnabled());
      params.setKeyStore(config.getSslKeyStore(), config.getSslKeyPass());
      params.setTrustStore(config.getSslTrustStore(), config.getSslTrustPass());
      return TSSLTransportFactory.getServerSocket(port, clientTimeout, ifAddress, params);
    } else {
      return new TServerSocket(bindAddr, config.getClientTimeout());
    }
  }

}
