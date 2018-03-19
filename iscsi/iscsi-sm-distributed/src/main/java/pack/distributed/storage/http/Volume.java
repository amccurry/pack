package pack.distributed.storage.http;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Volume {
  String name;
  long size;
  String iqn;
  String hdfsPath;
  String kafkaTopic;
}
