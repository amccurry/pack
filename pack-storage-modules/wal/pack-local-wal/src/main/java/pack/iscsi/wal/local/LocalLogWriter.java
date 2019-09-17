package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.FileIO;

public class LocalLogWriter implements Closeable {

  @Value
  @Builder(toBuilder = true)
  public static class LocalLogWriterConfig {
    File blockLogDir;

    @Builder.Default
    int bufferSize = 64 * 1024;
  }

  private final AtomicLong _lastGeneration = new AtomicLong(-1L);
  private final DataOutputStream _output;
  private final File _file;

  public LocalLogWriter(LocalLogWriterConfig config) throws IOException {
    _file = new File(config.getBlockLogDir(), UUID.randomUUID()
                                                  .toString());

    _output = new DataOutputStream(FileIO.createStream(_file, config.getBufferSize()));
  }

  public File getFile() {
    return _file;
  }

  public long getLastGeneration() {
    return _lastGeneration.get();
  }

  public void append(long generation, long position, byte[] bytes, int offset, int len) throws IOException {
    _lastGeneration.set(generation);
    int currentPosition = _output.size();
    _output.writeLong(generation);
    _output.writeLong(position);
    _output.writeInt(len);
    _output.write(bytes, offset, len);
    _output.writeInt(currentPosition);
  }

  @Override
  public void close() throws IOException {
    _output.close();
  }

  public int getSize() {
    return _output.size();
  }
}
