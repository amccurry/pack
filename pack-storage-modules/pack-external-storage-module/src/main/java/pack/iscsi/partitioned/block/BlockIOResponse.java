package pack.iscsi.partitioned.block;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockIOResponse {

  BlockState onDiskBlockState;

  long onDiskGeneration;

  long lastStoredGeneration;

  public static BlockIOResponse newBlockIOResult(long onDiskGeneration, BlockState onDiskState,
      long lastStoredGeneration) {
    return BlockIOResponse.builder()
                        .onDiskGeneration(onDiskGeneration)
                        .onDiskBlockState(onDiskState)
                        .lastStoredGeneration(lastStoredGeneration)
                        .build();
  }

}
