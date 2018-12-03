package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class WalKeyWritable implements Writable {

  public static enum Type {
    DATA((byte) 0), TRIM((byte) 1), NOOP((byte) 2);
    private final byte type;

    private Type(byte b) {
      type = b;
    }

    public byte getValue() {
      return type;
    }

    public static Type value(byte b) {
      switch (b) {
      case 0:
        return DATA;
      case 1:
        return TRIM;
      default:
        throw new RuntimeException("Can not find type for value " + b);
      }
    }
  }

  private Type type;
  private final LongWritable startingBlockId = new LongWritable();
  private final LongWritable endingBlockId = new LongWritable();

  public WalKeyWritable() {

  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public WalKeyWritable(long startingBlockId) {
    this.startingBlockId.set(startingBlockId);
    type = Type.DATA;
  }

  public WalKeyWritable(long startingBlockId, long endingBlockId) {
    this.startingBlockId.set(startingBlockId);
    this.endingBlockId.set(endingBlockId);
    type = Type.TRIM;
  }

  public long getStartingBlockId() {
    return startingBlockId.get();
  }

  public void setStartingBlockId(long blockId) {
    this.startingBlockId.set(blockId);
  }

  public long getEndingBlockId() {
    return endingBlockId.get();
  }

  public void setEtartingBlockId(long blockId) {
    this.endingBlockId.set(blockId);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.getValue());
    switch (type) {
    case NOOP:
      break;
    case TRIM:
      endingBlockId.write(out);
    case DATA:
      startingBlockId.write(out);
      break;
    default:
      break;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = Type.value(in.readByte());
    switch (type) {
    case NOOP:
      break;
    case TRIM:
      endingBlockId.readFields(in);
    case DATA:
      startingBlockId.readFields(in);
      break;
    default:
      break;
    }
  }

  @Override
  public String toString() {
    return "WalKeyWritable [type=" + type + ", startingBlockId=" + startingBlockId + ", endingBlockId=" + endingBlockId
        + "]";
  }

}
