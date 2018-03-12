package pack.distributed.storage.trace;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;

import com.amazonaws.util.Md5Utils;

import pack.distributed.storage.hdfs.BlockFile.Writer;

public class TraceWriter extends Writer {

  private final Writer _writer;
  private final Map<Integer, String> _hashMap;

  public TraceWriter(Map<Integer, String> hashMap, Writer writer) {
    _writer = writer;
    _hashMap = hashMap;
  }

  public void append(int longKey, BytesWritable value) throws IOException {
    byte[] bytes = value.getBytes();
    int length = value.getLength();
    _hashMap.put(longKey, Md5Utils.md5AsBase64(copy(bytes, length)));
    _writer.append(longKey, value);
  }

  private byte[] copy(byte[] bytes, int length) {
    if (bytes.length == length) {
      return bytes;
    }
    byte[] buf = new byte[length];
    System.arraycopy(bytes, 0, buf, 0, length);
    return bytes;
  }

  public void close() throws IOException {
    _writer.close();
  }

  public boolean canAppend(int longKey) throws IOException {
    return _writer.canAppend(longKey);
  }

  public void appendEmpty(int longKey) throws IOException {
    _writer.appendEmpty(longKey);
  }

  public long getLen() throws IOException {
    return _writer.getLen();
  }

}
