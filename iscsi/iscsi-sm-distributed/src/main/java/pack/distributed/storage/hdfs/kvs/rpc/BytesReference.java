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
public class BytesReference implements Writable {

  BytesRef bytesRef;

  public static BytesReference toBytesReference(BytesRef bytesRef) {
    return BytesReference.builder()
                         .bytesRef(bytesRef)
                         .build();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    RpcUtil.write(out, bytesRef);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    bytesRef = RpcUtil.readBytesRef(in);
  }

}
