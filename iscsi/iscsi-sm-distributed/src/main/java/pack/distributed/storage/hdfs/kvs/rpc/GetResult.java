package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import pack.distributed.storage.hdfs.kvs.BytesRef;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetResult implements Writable {

  private boolean found;
  private BytesRef value;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(found);
    RpcUtil.write(out, value);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    found = in.readBoolean();
    value = RpcUtil.readBytesRef(in);
  }

}
