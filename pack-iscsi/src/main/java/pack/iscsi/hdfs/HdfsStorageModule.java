package pack.iscsi.hdfs;

import java.io.IOException;

import pack.block.blockstore.BlockStore;
import pack.iscsi.BaseIStorageModule;

public class HdfsStorageModule extends BaseIStorageModule {

  private final BlockStore _store;

  public HdfsStorageModule(BlockStore store) throws IOException {
    super(store.getLength());
    _store = store;
  }

//  @Override
//  public void read(ProtocolDataUnit pdu, byte[] bytes, long storageIndex) throws IOException {
//    System.out.println("read  " + pdu);
//    super.read(pdu, bytes, storageIndex);
//  }
//
//  @Override
//  public void write(ProtocolDataUnit pdu, byte[] bytes, long storageIndex) throws IOException {
//    System.out.println("write " + pdu);
//    super.write(pdu, bytes, storageIndex);
//  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    int offset = 0;
    int length = bytes.length;
    while (length > 0) {
      int read = _store.read(storageIndex, bytes, offset, length);
      length -= read;
      offset += read;
      storageIndex += read;
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    int offset = 0;
    int length = bytes.length;
    while (length > 0) {
      int write = _store.write(storageIndex, bytes, offset, length);
      length -= write;
      offset += write;
      storageIndex += write;
    }
  }

  @Override
  public void close() throws IOException {
    _store.close();
  }

}
