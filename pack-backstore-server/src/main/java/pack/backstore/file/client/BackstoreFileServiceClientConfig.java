package pack.backstore.file.client;

import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BackstoreFileServiceClientConfig {

  String hostname;

  @Builder.Default
  int port = 8312;

  @Builder.Default
  int clientTimeout = (int) TimeUnit.SECONDS.toMillis(1000);

}
