package pack.distributed.storage.kafka.serialization;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import pack.distributed.storage.kafka.Block;
import pack.distributed.storage.kafka.Block.BlockBuilder;
import pack.distributed.storage.kafka.Blocks;
import pack.iscsi.storage.utils.PackUtils;

public class PackBlockDeserializer implements Deserializer<Blocks> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to do
  }

  @Override
  public Blocks deserialize(String topic, byte[] data) {
    int offset = 0;
    int length = PackUtils.getInt(data, offset);
    offset += 4;

    List<Block> blocks = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      BlockBuilder blockBuilder = Block.builder();

      // read block id
      blockBuilder.blockId(PackUtils.getInt(data, offset));
      offset += 4;

      // read trans id
      blockBuilder.transId(PackUtils.getLong(data, offset));
      offset += 8;

      // read block
      int len = PackUtils.getInt(data, offset);
      offset += 4;
      byte[] block = new byte[len];
      System.arraycopy(data, offset, block, 0, len);
      blockBuilder.data(block);
      offset += len;

      blocks.add(blockBuilder.build());
    }
    return Blocks.builder()
                 .blocks(blocks)
                 .build();
  }

  @Override
  public void close() {
    // nothing to do
  }

}
