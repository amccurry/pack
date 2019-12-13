package pack.backstore.coordinator.client;

import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

import lombok.Builder;
import lombok.Value;
import pack.thrift.common.ClientConfig;

@Value
@Builder(toBuilder = true)
public class CoordinatorServiceClientConfig implements ClientConfig<CoordinatorServiceClient> {

  String hostname;

  @Builder.Default
  int port = 8312;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(10);

  @Builder.Default
  int maxFrameLength = 1024 * 1024;

  @Override
  public CoordinatorServiceClient createClient(TProtocol protocol, TSocket transport) {
    return new CoordinatorServiceClient(protocol, transport);
  }

}
