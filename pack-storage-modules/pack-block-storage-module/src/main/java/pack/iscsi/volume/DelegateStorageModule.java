package pack.iscsi.volume;

import java.io.IOException;
import java.net.InetAddress;

import pack.iscsi.spi.StorageModule;

public abstract class DelegateStorageModule implements StorageModule {

  private final StorageModule _delegate;

  public DelegateStorageModule(StorageModule delegate) {
    _delegate = delegate;
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    return _delegate.checkBounds(logicalBlockAddress, transferLengthInBlocks);
  }

  @Override
  public long getSizeInBlocks() {
    return _delegate.getSizeInBlocks();
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    _delegate.read(bytes, storageIndex);
  }

  @Override
  public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    _delegate.read(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber);
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    _delegate.write(bytes, storageIndex);
  }

  @Override
  public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    _delegate.write(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber, dataSequenceNumber,
        targetTransferTag);
  }

  @Override
  public void flushWrites() throws IOException {
    _delegate.flushWrites();
  }

  @Override
  public int getBlockSize() {
    return _delegate.getBlockSize();
  }

  @Override
  public long getBlocks(long sizeInBytes) {
    return _delegate.getBlocks(sizeInBytes);
  }

  @Override
  public long getSizeInBytes() {
    return _delegate.getSizeInBytes();
  }

}
