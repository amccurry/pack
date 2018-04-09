package pack.distributed.storage.wal;

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pack.iscsi.storage.utils.PackUtils;

@Value
@Builder
@EqualsAndHashCode
public class Blocks {

  List<Block> blocks;

  public static byte[] toBytes(Blocks blocks) {
    int len = blocks.getMemorySize();
    byte[] buf = new byte[len];
    int offset = 0;
    PackUtils.putInt(buf, offset, blocks.getBlocks()
                                        .size());
    offset += 4;
    for (Block block : blocks.getBlocks()) {

      long transId = block.getTransId();
      PackUtils.putLong(buf, offset, transId);
      offset += 8;

      int blockId = block.getBlockId();
      PackUtils.putInt(buf, offset, blockId);
      offset += 4;

      byte[] data = block.getData();
      PackUtils.putInt(buf, offset, data.length);
      offset += 4;

      System.arraycopy(data, 0, buf, offset, data.length);
      offset += data.length;
    }
    return buf;
  }

  public int getMemorySize() {
    int mem = 4;
    for (Block block : blocks) {
      mem += block.getMemorySize();
    }
    return mem;
  }

  public static Blocks toBlocks(byte[] bs) {
    int offset = 0;
    int length = PackUtils.getInt(bs, offset);
    offset += 4;
    ImmutableList.Builder<Block> builder = ImmutableList.builder();
    for (int i = 0; i < length; i++) {
      long transId = PackUtils.getLong(bs, offset);
      offset += 8;

      int blockId = PackUtils.getInt(bs, offset);
      offset += 4;

      int len = PackUtils.getInt(bs, offset);
      offset += 4;

      byte[] buf = new byte[len];
      System.arraycopy(bs, offset, buf, 0, len);
      offset += len;
      builder.add(Block.builder()
                       .blockId(blockId)
                       .transId(transId)
                       .data(buf)
                       .build());
    }
    return Blocks.builder()
                 .blocks(builder.build())
                 .build();
  }

}
