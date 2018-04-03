package pack.distributed.storage.http;

import java.util.concurrent.atomic.AtomicReference;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class HttpServerConfig {

  PackDao packDao;
  int port;
  String address;
  AtomicReference<byte[]> textMetricsOutput;
}
