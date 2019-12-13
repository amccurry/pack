package pack.backstore.volume.client;

import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

import lombok.Builder;
import lombok.Value;
import pack.backstore.thrift.common.ClientConfig;
import pack.backstore.volume.server.VolumeServerConfig;

@Value
@Builder(toBuilder = true)
public class VolumeServiceClientConfig implements ClientConfig<VolumeServiceClient> {

  String hostname;

  @Builder.Default
  int port = VolumeServerConfig.DEFAULT_PORT;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(10);

  @Builder.Default
  int maxFrameLength = 1024 * 1024;

  @Override
  public VolumeServiceClient createClient(TProtocol protocol, TSocket transport) {
    return new VolumeServiceClient(protocol, transport);
  }

}
