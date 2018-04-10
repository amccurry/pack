package pack.distributed.storage.hdfs.kvs.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import pack.distributed.storage.hdfs.kvs.BytesRef;

public class RpcUtil {

  public static void write(DataOutput out, BytesRef ref) throws IOException {
    out.writeInt(ref.length);
    out.write(ref.bytes, ref.offset, ref.length);
  }

  public static BytesRef readBytesRef(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] buf = new byte[size];
    in.readFully(buf);
    return new BytesRef(buf);
  }

}
