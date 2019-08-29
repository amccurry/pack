package pack.iscsi.external.s3;

import com.google.common.base.Joiner;

public class S3Utils {

  private static final Joiner JOINER = Joiner.on('/');

  public static String getKey(String objectPrefix, long volumeId, long blockId, long generation) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(volumeId, blockId, generation);
    } else {
      return JOINER.join(objectPrefix, volumeId, blockId, generation);
    }
  }

}
