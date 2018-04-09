package pack.distributed.storage.status;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.storage.utils.PackUtils;

@Value
@Builder
public class BlockUpdateInfoBatch {
  List<BlockUpdateInfo> batch;
  String volume;

  public static BlockUpdateInfoBatch read(DataInput input) throws IOException {
    String volume = PackUtils.readString(input);
    List<BlockUpdateInfo> batch = new ArrayList<>();
    int count = input.readInt();
    for (int i = 0; i < count; i++) {
      batch.add(BlockUpdateInfo.read(input));
    }
    return BlockUpdateInfoBatch.builder()
                               .batch(batch)
                               .volume(volume)
                               .build();
  }

  public void write(DataOutput output) throws IOException {
    PackUtils.write(output, volume);
    int size = batch.size();
    output.writeInt(size);
    for (int i = 0; i < size; i++) {
      batch.get(i)
           .write(output);
    }
  }

}
