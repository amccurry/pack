package pack.distributed.storage.hdfs.kvs.rpc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder
public class Pair<T1, T2> {

  T1 t1;
  T2 t2;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <T1, T2> Pair<T1, T2> create(T1 t1, T2 t2) {
    return new Pair(t1, t2);
  }

}
