package pack.iscsi.storage.utils;

import java.io.IOException;
import java.net.InetAddress;

import org.jscsi.target.storage.IStorageModule;

import pack.iscsi.spi.StorageModule;

public class WrapperIStorageModule implements IStorageModule {

  private final StorageModule _storageModule;

  WrapperIStorageModule(StorageModule storageModule) {
    _storageModule = storageModule;
  }

  @Override
  public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
    return _storageModule.checkBounds(logicalBlockAddress, transferLengthInBlocks);
  }

  @Override
  public long getSizeInBlocks() {
    return _storageModule.getSizeInBlocks();
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    _storageModule.read(bytes, storageIndex);
  }

  @Override
  public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    _storageModule.read(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber);
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    _storageModule.write(bytes, storageIndex);
  }

  @Override
  public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    _storageModule.write(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber,
        dataSequenceNumber, targetTransferTag);
  }

  @Override
  public void close() throws IOException {
    _storageModule.close();
  }

  @Override
  public void flushWrites() throws IOException {
    _storageModule.flushWrites();
  }

  public static IStorageModule create(StorageModule storageModule) {
    return new WrapperIStorageModule(storageModule);
  }

}
