package pack.distributed.storage.status;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BlockUpdateInfo {
  int blockId;
  long transId;

  public void write(DataOutput output) throws IOException {
    output.writeInt(blockId);
    output.writeLong(transId);
  }

  public static BlockUpdateInfo read(DataInput input) throws IOException {
    return BlockUpdateInfo.builder()
                          .blockId(input.readInt())
                          .transId(input.readLong())
                          .build();
  }
}
