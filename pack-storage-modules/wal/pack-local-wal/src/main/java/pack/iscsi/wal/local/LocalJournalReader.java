package pack.iscsi.wal.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

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
  private final long _minGeneration;
  private final long _maxGeneration;
  private final File _file;

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
    ra.seek(0);
    return ra.readLong();
  }

  private static long getMaxGeneration(RandomAccessIO ra) throws IOException {
    ra.seek(ra.length() - 4);
    long position = ra.readInt();
    ra.seek(position);
    return ra.readLong();
  }

  public String getUuid() {
    return _file.getName();
  }

}