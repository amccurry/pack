package pack.s3.server;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;

import pack.s3.server.thrift.TServerDomainSocket;
import pack.s3.thrift.PackService;

public class PackServer implements PackService.Iface {

  public static void main(String[] args) throws TTransportException {
    PackService.Processor<PackService.Iface> processor = new PackService.Processor<>(new PackServer());

    // TServerTransport serverTransport = new TServerSocket(9090);
    File path = new File("/tmp/pack.sock");
    TServerDomainSocket serverTransport = new TServerDomainSocket(path);
    
    Args serverArgs = new Args(serverTransport);
    serverArgs.protocolFactory(new TBinaryProtocol.Factory());
    serverArgs.processor(processor);
    serverArgs.transportFactory(new TFramedTransport.Factory());
    TServer server = new TSimpleServer(serverArgs);
    server.serve();
  }

  @Override
  public long open(String path) throws TException {
    System.out.println(path);
    return 0;
  }

  @Override
  public ByteBuffer read_data(long fh, long position) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void write_data(long fh, long position, ByteBuffer buffer) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void delete_data(long fh, long position, long length) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void release(long fh) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public List<String> volumes() throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public List<String> mounts() throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void create(String volumeName) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void destroy(String volumeName) throws TException {
    throw new RuntimeException("Not Implemented");
  }

  @Override
  public void resize(String volumeName, long size) throws TException {
    throw new RuntimeException("Not Implemented");
  }

}
