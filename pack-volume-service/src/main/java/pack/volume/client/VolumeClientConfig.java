package pack.volume.client;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class VolumeClientConfig {

  String hostname;

  @Builder.Default
  int port = 8311;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(1000);

  @Builder.Default
  int maxClientCount = 10;

}
