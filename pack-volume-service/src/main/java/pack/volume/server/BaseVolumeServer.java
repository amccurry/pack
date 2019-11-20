package pack.volume.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.volume.curator.CuratorUtil;
import pack.volume.thrift.generated.PackVolumeException;
import pack.volume.thrift.generated.PackVolumeService;
import pack.volume.thrift.generated.PackVolumeService.Processor;

public abstract class BaseVolumeServer implements PackVolumeService.Iface, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseVolumeServer.class);

  private static final String _0_0_0_0 = "0.0.0.0";
  private static final String SERVER = "server";

  private final TServerTransport _serverTransport;
  private final TServer _server;
  private final int _minThreads;
  private final int _maxThreads;
  private final CuratorFramework _curatorFramework;
  private final String _zkPrefix;

  public BaseVolumeServer(VolumeServerConfig config) throws TTransportException {
    _minThreads = config.getMinThreads();
    _maxThreads = config.getMaxThreads();
    _curatorFramework = config.getCuratorFramework();
    _zkPrefix = config.getZkPrefix();
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
    CuratorUtil.registerServer(_curatorFramework, _zkPrefix, getBindInetAddress(), getBindPort());
    if (blocking) {
      thread.join();
    }
  }

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

  @Override
  public void noop() throws PackVolumeException, TException {

  }

  protected void validateBrickId(String brickId) throws IOException {
    boolean valid = true;
    char[] a = brickId.toCharArray();
    for (char c : a) {
      valid = ((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || ((c >= '0') && (c <= '9')) || (c == ':');
      if (!valid) {
        throw new IOException(brickId + " contains bad chars");
      }
    }
  }

  protected PackVolumeException newPackBrickException(Exception e) {
    StringWriter writer = new StringWriter();
    try (PrintWriter pw = new PrintWriter(writer)) {
      e.printStackTrace(pw);
    }
    return new PackVolumeException(e.getMessage(), writer.toString());
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

  private TServer createServer(VolumeServerConfig config) {
    Processor<BaseVolumeServer> processor = new PackVolumeService.Processor<>(this);
    TFramedTransport.Factory framedTransportFactory = new TFramedTransport.Factory(1024 * 1024);
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();

    TTransportFactory transportFactory = framedTransportFactory;

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(_serverTransport);
    args.processor(handleClosedConnections(processor))
        .protocolFactory(protocolFactory)
        .transportFactory(transportFactory)
        .minWorkerThreads(_minThreads)
        .maxWorkerThreads(_maxThreads);
    return new TThreadPoolServer(args);
  }

  private TServerTransport createServerTransport(VolumeServerConfig config, InetSocketAddress bindAddr)
      throws TTransportException {
    return new TServerSocket(bindAddr, config.getClientTimeout());
  }

}
