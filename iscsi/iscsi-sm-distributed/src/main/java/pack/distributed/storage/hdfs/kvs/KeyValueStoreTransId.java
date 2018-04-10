package pack.distributed.storage.hdfs.kvs;

import org.apache.hadoop.fs.Path;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class KeyValueStoreTransId {

  long position;
  Path path;

}
