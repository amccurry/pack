package pack.block.server.admin.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jnr.unixsocket.UnixSocket;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;
import pack.block.server.admin.BlockPackAdmin;
import pack.block.server.admin.PidResponse;
import pack.block.server.admin.Status;
import pack.block.server.admin.StatusResponse;
import spark.Service;
import spark.SparkJava;
import spark.SparkJavaIdentifier;

public class BlockPackAdminClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackAdminClient.class);

  private static final String CONNECTION_REFUSED = "connection refused";
  private static final String UTF_8 = "UTF-8";
  private static final String HTTP_LOCALHOST = "http://localhost";

  public static void main(String[] args) throws IOException, InterruptedException {
    File file = new File("test.sock");
    file.delete();
    BlockPackAdminClient client = new BlockPackAdminClient(file);
    printPid(client);
    startTestServer(file);
    for (int i = 0; i < 10; i++) {
      printPid(client);
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
    System.exit(0);
  }

  private static void printPid(BlockPackAdminClient client) throws IOException {
    try {
      System.out.println(client.getPid());
    } catch (NoFileException e) {
      System.out.println("Server doesn't exist");
    } catch (ConnectionRefusedException e) {
      System.out.println("Server offline");
    }
  }

  private final ObjectMapper _mapper = new ObjectMapper();
  private final File _sockFile;

  public static BlockPackAdminClient create(File sockFile) {
    return new BlockPackAdminClient(sockFile);
  }

  public BlockPackAdminClient(File sockFile) {
    _sockFile = sockFile;
  }

  public Status getStatus() throws IOException {
    LOGGER.info("getStatus {}", _sockFile);
    GetMethod get = new GetMethod(HTTP_LOCALHOST + BlockPackAdmin.STATUS);
    int executeMethod = getClient().executeMethod(get);
    String body = getBodyAsString(get);
    if (executeMethod == 200) {
      StatusResponse statusResponse = _mapper.readValue(body, StatusResponse.class);
      return statusResponse.getStatus();
    } else {
      throw new IOException(body);
    }
  }

  public String getPid() throws IOException {
    LOGGER.info("getPid {}", _sockFile);
    GetMethod get = new GetMethod(HTTP_LOCALHOST + BlockPackAdmin.PID);
    int executeMethod = getClient().executeMethod(get);
    String body = getBodyAsString(get);
    if (executeMethod == 200) {
      return body;
    } else {
      throw new IOException(body);
    }
  }

  private HttpClient getClient() {
    return createHttpClient(_sockFile);
  }

  public void umount() throws IOException {
    LOGGER.info("umount {}", _sockFile);
    PostMethod post = new PostMethod(HTTP_LOCALHOST + BlockPackAdmin.UMOUNT);
    int executeMethod = getClient().executeMethod(post);
    if (executeMethod != 200) {
      throw new IOException(getBodyAsString(post));
    }
  }

  public void shutdown() throws IOException {
    LOGGER.info("shutdown {}", _sockFile);
    PostMethod post = new PostMethod(HTTP_LOCALHOST + BlockPackAdmin.SHUTDOWN);
    int executeMethod = getClient().executeMethod(post);
    if (executeMethod != 200) {
      throw new IOException(getBodyAsString(post));
    }
  }

  private static HttpClient createHttpClient(File path) {
    HttpClient client = new HttpClient();
    client.setHttpConnectionManager(getConnectionManager(path));
    return client;
  }

  private static HttpConnectionManager getConnectionManager(File path) {
    return new HttpConnectionManager() {

      @Override
      public void setParams(HttpConnectionManagerParams params) {
        throw new RuntimeException();
      }

      @Override
      public void releaseConnection(HttpConnection conn) {
        conn.close();
      }

      @Override
      public HttpConnectionManagerParams getParams() {
        return new HttpConnectionManagerParams();
      }

      @Override
      public HttpConnection getConnectionWithTimeout(HostConfiguration hostConfiguration, long timeout)
          throws ConnectionPoolTimeoutException {
        ProtocolSocketFactory factory = new ProtocolSocketFactory() {

          @Override
          public Socket createSocket(String host, int port, InetAddress localAddress, int localPort,
              HttpConnectionParams params) throws IOException, UnknownHostException, ConnectTimeoutException {
            if (!path.exists()) {
              throw new NoFileException();
            }
            UnixSocketAddress addr = new UnixSocketAddress(path);
            try {
              return new UnixSocket(UnixSocketChannel.open(addr));
            } catch (IOException e) {
              if (CONNECTION_REFUSED.equals(e.getMessage()
                                             .toLowerCase()
                                             .trim())) {
                throw new ConnectionRefusedException();
              }
              throw e;
            }
          }

          @Override
          public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
              throws IOException, UnknownHostException {
            throw new RuntimeException();
          }

          @Override
          public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
            throw new RuntimeException();
          }
        };
        Protocol protocol = new Protocol("http", factory, 80);
        HttpConnection httpConnection = new HttpConnection("localhost", 80, protocol);
        httpConnection.setHttpConnectionManager(this);
        return httpConnection;
      }

      @Override
      public HttpConnection getConnection(HostConfiguration hostConfiguration, long timeout) throws HttpException {
        throw new RuntimeException();
      }

      @Override
      public HttpConnection getConnection(HostConfiguration hostConfiguration) {
        throw new RuntimeException();
      }

      @Override
      public void closeIdleConnections(long idleTimeout) {
        throw new RuntimeException();
      }
    };
  }

  private static Service startTestServer(File sockFile) {
    SparkJava.init();
    Service service = Service.ignite();
    SparkJava.configureService(SparkJavaIdentifier.UNIX_SOCKET, service);
    String pid = BlockPackAdmin.getPid();
    service.ipAddress(sockFile.getAbsolutePath());
    service.get(BlockPackAdmin.PID, (request, response) -> PidResponse.builder()
                                                                      .pid(pid)
                                                                      .build(),
        BlockPackAdmin.TRANSFORMER);
    return service;
  }

  private static String getBodyAsString(HttpMethodBase base) throws IOException {
    try (InputStream input = base.getResponseBodyAsStream()) {
      return IOUtils.toString(input, UTF_8);
    }
  }
}
