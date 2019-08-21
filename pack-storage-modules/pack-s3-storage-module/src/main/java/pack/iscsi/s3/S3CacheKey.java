package pack.iscsi.s3;

import com.google.common.base.Joiner;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class S3CacheKey {
  private static final Joiner JOINER = Joiner.on('/');

  String name;
  long blockId;
  String s3Bucket;
  String s3ObjectPrefix;
  int blockSize;

  public String getS3Key() {
    if (s3ObjectPrefix == null || s3ObjectPrefix.trim()
                                                .isEmpty()) {
      return JOINER.join(name, Long.toString(blockId));
    } else {
      return JOINER.join(s3ObjectPrefix.trim(), name, Long.toString(blockId));
    }
  }
}
