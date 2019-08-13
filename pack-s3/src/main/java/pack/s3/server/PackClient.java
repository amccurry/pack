package pack.s3.server;

import java.io.File;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;

import pack.s3.server.thrift.TDomainSocket;
import pack.s3.thrift.PackService;
import pack.s3.thrift.PackService.Client;

public class PackClient {

  public static void main(String[] args) throws TException {
    // TSocket tSocket = new TSocket("localhost", 9090);
    // tSocket.open();
    File path = new File("/tmp/pack.sock");
    TDomainSocket tSocket = new TDomainSocket(path);

    TTransport trans = new TFramedTransport(tSocket);
    TBinaryProtocol protocol = new TBinaryProtocol(trans);
    Client client = new PackService.Client(protocol);
    while (true) {
      client.open("/test");
    }
  }

}
