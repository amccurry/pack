package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.FileIO;
import pack.iscsi.spi.RandomAccessIO;

public class LocalLogReader implements Comparable<LocalLogReader>, Closeable {

  private static final String R = "r";

  @Value
  @Builder(toBuilder = true)
  public static class LocalLogReaderConfig {
    File blockLogFile;

    @Builder.Default
    int bufferSize = 64 * 1024;
  }

  private final RandomAccessIO _ra;
  private final long _minGeneration;
  private final long _maxGeneration;
  private final File _file;

  private long _generation;
  private long _position;
  private byte[] _bytes;
  private int _length;
  private int _recordPosition;

  public LocalLogReader(LocalLogReaderConfig config) throws IOException {
    _file = config.getBlockLogFile();
    int bufferSize = config.getBufferSize();
    _ra = FileIO.openRandomAccess(_file, bufferSize, R);
    _minGeneration = getMinGeneration(_ra);
    _maxGeneration = getMaxGeneration(_ra);
  }

  public File getFile() {
    return _file;
  }

  public long getMinGeneration() {
    return _minGeneration;
  }

  public long getMaxGeneration() {
    return _maxGeneration;
  }

  public void reset() throws IOException {
    _ra.seek(0);
  }

  public boolean next() throws IOException {
    if (_ra.getFilePointer() == _ra.length()) {
      return false;
    }
    _generation = _ra.readLong();
    _position = _ra.readLong();
    _length = _ra.readInt();
    growBufferIfNeeded();
    _ra.readFully(_bytes, 0, _length);
    _recordPosition = _ra.readInt();
    return true;
  }

  public int getRecordPosition() {
    return _recordPosition;
  }

  public long getGeneration() {
    return _generation;
  }

  public long getPosition() {
    return _position;
  }

  public byte[] getBytes() {
    return _bytes;
  }

  public int getLength() {
    return _length;
  }

  @Override
  public void close() throws IOException {
    _ra.close();
  }

  private void growBufferIfNeeded() {
    if (_bytes == null || _bytes.length < _length) {
      _bytes = new byte[_length];
    }
  }

  @Override
  public int compareTo(LocalLogReader o) {
    return Long.compare(_minGeneration, o.getMinGeneration());
  }

  private static long getMinGeneration(RandomAccessIO ra) throws IOException {
    ra.seek(0);
    return ra.readLong();
  }

  private static long getMaxGeneration(RandomAccessIO ra) throws IOException {
    ra.seek(ra.length() - 4);
    long position = ra.readInt();
    ra.seek(position);
    return ra.readLong();
  }

}