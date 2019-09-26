package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Builder;
import lombok.Value;
import pack.iscsi.io.FileIO;
import pack.iscsi.spi.RandomAccessIO;

public class LocalJournalReader implements Comparable<LocalJournalReader>, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalJournalReader.class);

  private static final String R = "r";

  @Value
  @Builder(toBuilder = true)
  public static class LocalLogReaderConfig {
    File blockLogFile;

    @Builder.Default
    int bufferSize = 64 * 1024;
  }

  private final RandomAccessIO _ra;
  private final AtomicLong _raPosition = new AtomicLong();
  private final long _minGeneration;
  private final long _maxGeneration;
  private final File _file;
  private final ByteBuffer _buffer = ByteBuffer.allocate(8);

  private long _generation;
  private long _position;
  private byte[] _bytes;
  private int _length;
  private int _recordPosition;

  public LocalJournalReader(LocalLogReaderConfig config) throws IOException {
    _file = config.getBlockLogFile();
    int bufferSize = config.getBufferSize();
    _ra = FileIO.openRandomAccess(_file, bufferSize, R);
    LOGGER.info("Opening journal reader file {} length {}", _file, _file.length());
    _minGeneration = getMinGeneration(_ra);
    try {
      _maxGeneration = getMaxGeneration(_ra);
    } catch (Exception e) {
      LOGGER.error("Unknown error", e);
      throw new IOException("Error get max generation file " + _file + " length " + _file.length());
    }
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
    _raPosition.set(0);
  }

  public boolean next() throws IOException {
    if (_raPosition.get() == _ra.length()) {
      return false;
    }
    _generation = readLong();
    _position = readLong();
    _length = readInt();
    growBufferIfNeeded();
    readFully(_bytes, 0, _length);
    _recordPosition = readInt();
    return true;
  }

  private void readFully(byte[] bytes, int offset, int length) throws IOException {
    long pos = _raPosition.get();
    _ra.readFully(pos, bytes, offset, length);
    _raPosition.set(pos + length);
  }

  private int readInt() throws IOException {
    long pos = _raPosition.get();
    _ra.readFully(pos, _buffer.array(), 0, 4);
    _raPosition.set(pos + 4);
    return _buffer.getInt(0);
  }

  private long readLong() throws IOException {
    long pos = _raPosition.get();
    _ra.readFully(pos, _buffer.array(), 0, 8);
    _raPosition.set(pos + 8);
    return _buffer.getLong(0);
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
    LOGGER.info("Closing journal reader file {} length {}", _file, _file.length());
    _ra.close();
  }

  private void growBufferIfNeeded() {
    if (_bytes == null || _bytes.length < _length) {
      _bytes = new byte[_length];
    }
  }

  @Override
  public int compareTo(LocalJournalReader o) {
    return Long.compare(_minGeneration, o.getMinGeneration());
  }

  private static long getMinGeneration(RandomAccessIO ra) throws IOException {
    byte[] buffer = new byte[8];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    ra.readFully(0, buffer, 0, 8);
    return byteBuffer.getLong(0);
  }

  private static long getMaxGeneration(RandomAccessIO ra) throws IOException {
    byte[] buffer = new byte[8];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    ra.readFully(ra.length() - 4, buffer, 0, 4);
    long position = byteBuffer.getInt(0);
    ra.readFully(position, buffer, 0, 8);
    return byteBuffer.getLong(0);
  }

  public String getUuid() {
    return _file.getName();
  }

}