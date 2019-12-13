package pack.backstore.thrift.common;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

public interface ClientConfig<T> {

  String getHostname();

  int getPort();

  int getClientTimeout();
  
  int getMaxFrameLength();

  T createClient(TProtocol protocol, TSocket transport);

}
