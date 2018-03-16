package pack.distributed.storage.kafka.serialization;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import pack.distributed.storage.kafka.Block;
import pack.distributed.storage.kafka.Blocks;
import pack.iscsi.storage.utils.PackUtils;

public class PackBlockSerializer implements Serializer<Blocks> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to do
  }

  @Override
  public byte[] serialize(String topic, Blocks data) {
    int length = getLength(data);
    byte[] buf = new byte[length];
    List<Block> blocks = data.getBlocks();
    int offset = 0;
    PackUtils.putInt(buf, offset, blocks.size());
    offset += 4;
    for (Block block : blocks) {
      // write block id
      PackUtils.putInt(buf, offset, block.getBlockId());
      offset += 4;

      // write trans id
      PackUtils.putLong(buf, offset, block.getTransId());
      offset += 8;

      // write block
      byte[] b = block.getData();
      int len = b.length;
      PackUtils.putInt(buf, offset, len);
      offset += 4;
      System.arraycopy(b, 0, buf, offset, len);
      offset += len;
    }
    return buf;
  }

  private int getLength(Blocks data) {
    int length = 4;
    List<Block> blocks = data.getBlocks();
    for (Block block : blocks) {
      length += getLength(block);
    }
    return length;
  }

  private int getLength(Block block) {
    return block.getData().length + 4 + 8 + 4;
  }

  @Override
  public void close() {
    // nothing to do
  }

}
