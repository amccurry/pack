package pack.iscsi.brick;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class BrickControllerConfig {

  long volumeId;

  @Builder.Default
  int byteBufferSize = 1024 * 1024;

  @Builder.Default
  int dataPartCount = 4;

  @Builder.Default
  int parityPartCount = 2;

  @Builder.Default
  int blockSize = 4096;

}
