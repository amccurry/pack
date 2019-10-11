package pack.iscsi.io.direct;

import java.io.File;
import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;

public class BlockParitionedDirectIO implements RandomAccessIO {

  private final DirectIO _directIO;
  private final int _blockSize;

  public BlockParitionedDirectIO(File file, int blockSize) throws IOException {
    _directIO = new DirectIO(file);
    _blockSize = blockSize;
  }

  @Override
  public void close() throws IOException {
    _directIO.close();
  }

  @Override
  public void write(long position, byte[] buffer, int offset, int length) throws IOException {
    while (length > 0) {
      long blockOffset = getBlockOffset(position);
      int len = (int) Math.min(length, _blockSize - blockOffset);
      _directIO.write(position, buffer, offset, len);
      length -= len;
      offset += len;
      position += len;
    }
  }

  private long getBlockOffset(long position) {
    return position % _blockSize;
  }

  @Override
  public void read(long position, byte[] buffer, int offset, int length) throws IOException {
    while (length > 0) {
      long blockOffset = getBlockOffset(position);
      int len = (int) Math.min(length, _blockSize - blockOffset);
      _directIO.read(position, buffer, offset, len);
      length -= len;
      offset += len;
      position += len;
    }
  }

  @Override
  public void setLength(long length) throws IOException {
    if (length % _blockSize != 0) {
      throw new IOException("New length is not a multiple of blockSize " + _blockSize);
    }
    _directIO.setLength(length);
  }

  @Override
  public void flush() throws IOException {
    _directIO.flush();
  }

  @Override
  public long length() throws IOException {
    return _directIO.length();
  }

  @Override
  public RandomAccessIOReader cloneReadOnly() throws IOException {
    return _directIO.cloneReadOnly();
  }

}
