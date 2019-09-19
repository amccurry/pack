package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.FileIO;
import pack.iscsi.spi.RandomAccessIO;

public class LocalJournalWriter implements Closeable {

  private static final String RW = "rw";

  @Value
  @Builder(toBuilder = true)
  public static class LocalLogWriterConfig {
    File blockLogDir;

    @Builder.Default
    int bufferSize = 64 * 1024;
  }

  private final AtomicLong _lastGeneration = new AtomicLong(-1L);
  private final File _file;
  private final RandomAccessIO _randomAccessIO;
  private final AtomicInteger _size = new AtomicInteger();

  public LocalJournalWriter(LocalLogWriterConfig config) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    _file = new File(config.getBlockLogDir(), uuid);
    _randomAccessIO = FileIO.openRandomAccess(_file, config.getBufferSize(), RW);
  }

  public File getFile() {
    return _file;
  }

  public long getLastGeneration() {
    return _lastGeneration.get();
  }

  public synchronized void append(long generation, long position, byte[] bytes, int offset, int len)
      throws IOException {
    _lastGeneration.set(generation);
    int currentPosition = _size.get();
    int bufferLength = 8 + 8 + 4 + len + 4;
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferLength);
    byteBuffer.putLong(generation)
              .putLong(position)
              .putInt(len)
              .put(bytes, offset, len)
              .putInt(currentPosition);
    _randomAccessIO.write(byteBuffer.array());
    _size.addAndGet(bufferLength);
  }

  @Override
  public void close() throws IOException {
    int size = getSize();
    _randomAccessIO.close();
    if (_file.length() != size) {
      throw new IOException(
          "Expected length of " + size + " is not actual length " + _file.length() + " for file " + _file);
    }
  }

  public int getSize() {
    return _size.get();
  }
}
