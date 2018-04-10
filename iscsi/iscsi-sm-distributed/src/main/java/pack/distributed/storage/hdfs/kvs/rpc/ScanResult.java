package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.ImmutableList;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import pack.distributed.storage.hdfs.kvs.BytesRef;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScanResult implements Writable {

  List<Pair<BytesRef, BytesRef>> result;

  @Override
  public void write(DataOutput out) throws IOException {
    int size = result.size();
    out.writeInt(size);
    for (Pair<BytesRef, BytesRef> pair : result) {
      RpcUtil.write(out, pair.getT1());
      RpcUtil.write(out, pair.getT2());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    ImmutableList.Builder<Pair<BytesRef, BytesRef>> builder = ImmutableList.builder();
    for (int i = 0; i < size; i++) {
      BytesRef t1 = RpcUtil.readBytesRef(in);
      BytesRef t2 = RpcUtil.readBytesRef(in);
      builder.add(Pair.create(t1, t2));
    }
    result = builder.build();
  }

}
