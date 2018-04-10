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
public class Key implements Writable {

  BytesRef key;

  public static Key toKey(BytesRef key) {
    return Key.builder()
              .key(key)
              .build();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    RpcUtil.write(out, key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key = RpcUtil.readBytesRef(in);
  }

}
