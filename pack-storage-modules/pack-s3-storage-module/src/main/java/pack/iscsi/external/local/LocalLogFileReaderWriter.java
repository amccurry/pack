package pack.iscsi.external.local;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class LocalLogFileReaderWriter {

  public static class LocalLogReader implements Comparable<LocalLogReader>, Closeable {

    private final RandomAccessFile _raf;
    private final long _minGeneration;
    private final long _maxGeneration;
    private final File _file;

    private long _generation;
    private long _position;
    private byte[] _bytes;
    private int _length;
    private int _recordPosition;

    public LocalLogReader(File blockLogFile) throws IOException {
      _file = blockLogFile;
      _raf = new RandomAccessFile(blockLogFile, "r");
      _minGeneration = LocalLogFileReaderWriter.getMinGeneration(_raf);
      _maxGeneration = LocalLogFileReaderWriter.getMaxGeneration(_raf);
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
      _raf.seek(0);
    }

    public boolean next() throws IOException {
      if (_raf.getFilePointer() == _raf.length()) {
        return false;
      }
      _generation = _raf.readLong();
      _position = _raf.readLong();
      _length = _raf.readInt();
      growBufferIfNeeded();
      _raf.readFully(_bytes, 0, _length);
      _recordPosition = _raf.readInt();
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
      _raf.close();
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

  }

  private static long getMinGeneration(RandomAccessFile raf) throws IOException {
    raf.seek(0);
    return raf.readLong();
  }

  private static long getMaxGeneration(RandomAccessFile raf) throws IOException {
    raf.seek(raf.length() - 4);
    long position = raf.readInt();
    raf.seek(position);
    return raf.readLong();
  }

  public static class LocalLogWriter implements Closeable {

    private final DataOutputStream _output;
    private final AtomicLong _lastGeneration = new AtomicLong(-1L);

    public LocalLogWriter(File blockLogDir) throws IOException {
      File file = new File(blockLogDir, UUID.randomUUID()
                                            .toString());
      _output = new DataOutputStream(new FileOutputStream(file));
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
}
