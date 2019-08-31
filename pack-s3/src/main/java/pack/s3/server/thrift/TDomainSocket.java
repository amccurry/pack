package pack.s3.server.thrift;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import jnr.unixsocket.UnixSocket;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

public class TDomainSocket extends TSocket {

  public TDomainSocket(File path) throws TTransportException {
    super(getSocket(path));
  }

  private static Socket getSocket(File path) throws TTransportException {
    UnixSocketAddress address = new UnixSocketAddress(path);
    UnixSocketChannel channel;
    try {
      channel = UnixSocketChannel.open(address);
    } catch (IOException e) {
      throw new TTransportException(TTransportException.UNKNOWN, "Domain Socket file " + path + " not found.", e);
    }
    return new UnixSocket(channel);
  }

}
