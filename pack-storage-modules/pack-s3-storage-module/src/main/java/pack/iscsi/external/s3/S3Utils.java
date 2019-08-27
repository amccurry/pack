package pack.iscsi.external.s3;

import com.google.common.base.Joiner;

import pack.iscsi.partitioned.block.BlockIORequest;

public class S3Utils {

  private static final Joiner JOINER = Joiner.on('/');

  public static String getKey(String objectPrefix, BlockIORequest request) {
    if (objectPrefix == null || objectPrefix.trim()
                                            .isEmpty()) {
      return JOINER.join(request.getVolumeId(), request.getBlockId(), request.getOnDiskGeneration());
    } else {
      return JOINER.join(objectPrefix, request.getVolumeId(), request.getBlockId(), request.getOnDiskGeneration());
    }
  }

}
