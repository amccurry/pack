package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import pack.distributed.storage.hdfs.kvs.KeyValueStoreTransId;
import pack.iscsi.storage.utils.PackUtils;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransId implements Writable {

  long position;
  String path;

  public static TransId toTransId(KeyValueStoreTransId transId) {
    return TransId.builder()
                  .path(transId.getPath()
                               .toString())
                  .position(transId.getPosition())
                  .build();
  }

  public static KeyValueStoreTransId toKeyValueStoreTransId(TransId transId) {
    return KeyValueStoreTransId.builder()
                               .path(new Path(transId.getPath()))
                               .position(transId.getPosition())
                               .build();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(getPosition());
    PackUtils.write(out, path);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    position = in.readLong();
    path = PackUtils.readString(in);
  }

}
