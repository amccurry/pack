package pack.thrift.common;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;

import pack.util.IOUtils;

public abstract class BaseServer implements Closeable {

  private static final String _0_0_0_0 = "0.0.0.0";
  private static final String SERVER = "server";

  private final Logger _logger;
  private final TServerTransport _serverTransport;
  private final TServer _server;
  private final int _minThreads;
  private final int _maxThreads;

  public BaseServer(Logger logger, String hostname, int port, int clientTimeout, int minThreads, int maxThreads)
      throws TTransportException {
    _logger = logger;
    _minThreads = minThreads;
    _maxThreads = maxThreads;
    InetSocketAddress bindAddr = new InetSocketAddress(hostname, port);
    _serverTransport = createServerTransport(bindAddr, clientTimeout);
    _server = createServer();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(_logger, () -> _server.stop());
  }

  public void start(boolean blocking) throws Exception {
    Thread thread = new Thread(() -> _server.serve());
    thread.setName(SERVER);
    thread.setDaemon(true);
    thread.start();
    while (!_server.isServing()) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    _logger.info("Listening on {} port {}", getBindInetAddress(), getBindPort());
    serverStarted(getBindInetAddress(), getBindPort());
    if (blocking) {
      thread.join();
    }
  }

  protected void serverStarted(InetAddress bindInetAddress, int bindPort) {

  }

  protected abstract TProcessor createTProcessor();

  public InetAddress getBindInetAddress() {
    if (_serverTransport instanceof TServerSocket) {
      return resolveToAddress(((TServerSocket) _serverTransport).getServerSocket()
                                                                .getInetAddress());
    } else {
      return null;
    }
  }

  private InetAddress resolveToAddress(InetAddress inetAddress) {
    String hostAddress = inetAddress.getHostAddress();
    if (hostAddress.equals(_0_0_0_0)) {
      try {
        return InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }
    return inetAddress;
  }

  public int getBindPort() {
    if (_serverTransport instanceof TServerSocket) {
      return ((TServerSocket) _serverTransport).getServerSocket()
                                               .getLocalPort();
    } else {
      return -1;
    }
  }

  private TProcessor handleClosedConnections(TProcessor processor) {
    return (in, out) -> {
      try {
        return processor.process(in, out);
      } catch (TTransportException e) {
        switch (e.getType()) {
        case TTransportException.END_OF_FILE:
          _logger.debug("Client closed connection");
          return false;
        case TTransportException.UNKNOWN:
          _logger.debug("Client connection terminated, possible timeout");
          return false;
        default:
          throw e;
        }
      }
    };
  }

  private TServer createServer() {
    TProcessor processor = createTProcessor();
    TFramedTransport.Factory framedTransportFactory = new TFramedTransport.Factory(1024 * 1024);
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(_serverTransport);
    args.processor(handleClosedConnections(processor))
        .protocolFactory(protocolFactory)
        .transportFactory(framedTransportFactory)
        .minWorkerThreads(_minThreads)
        .maxWorkerThreads(_maxThreads);
    return new TThreadPoolServer(args);
  }

  protected TServerTransport createServerTransport(InetSocketAddress bindAddr, int clientTimeout)
      throws TTransportException {
    return new TServerSocket(bindAddr, clientTimeout);
  }

}
