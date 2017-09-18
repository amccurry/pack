package pack.block.server.admin.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.ConnectionPoolTimeoutException;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import jnr.unixsocket.UnixSocket;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class UnixDomainSocketClient {

  protected static final String HTTP_LOCALHOST = "http://localhost";
  protected static final String CONNECTION_REFUSED = "connection refused";
  protected static final String UTF_8 = "UTF-8";
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final File _sockFile;

  public UnixDomainSocketClient(File sockFile) {
    _sockFile = sockFile;
  }

  protected HttpClient getClient() {
    return createHttpClient(_sockFile);
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

  protected static String getBodyAsString(HttpMethodBase base) throws IOException {
    try (InputStream input = base.getResponseBodyAsStream()) {
      return IOUtils.toString(input, UTF_8);
    }
  }
}
