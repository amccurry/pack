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
import pack.iscsi.storage.utils.PackUtils;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StoreList implements Writable {

  List<Pair<String, String>> stores;

  @Override
  public void write(DataOutput out) throws IOException {
    int size = stores.size();
    out.writeInt(size);
    for (Pair<String, String> store : stores) {
      PackUtils.write(out, store.getT1());
      PackUtils.write(out, store.getT2());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ImmutableList.Builder<Pair<String, String>> builder = ImmutableList.builder();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      builder.add(Pair.create(PackUtils.readString(in), PackUtils.readString(in)));
    }
    stores = builder.build();
  }

}
