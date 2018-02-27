package pack.iscsi.hdfs;

import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.BlockStore;
import pack.iscsi.BaseIStorageModule;

public class HdfsStorageModule extends BaseIStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsStorageModule.class);

  private final BlockStore _store;

  public HdfsStorageModule(BlockStore store) throws IOException {
    super(store.getLength());
    _store = store;
  }

  @Override
  public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber) throws IOException {
    LOGGER.debug("{} {} read initTag {} comSeqNum {} index {} length {} ", address, port, initiatorTaskTag,
        commandSequenceNumber, storageIndex, bytes.length);
    try {
      read(bytes, storageIndex);
    } catch (IOException e) {
      LOGGER.error("Unknown error", e);
      throw e;
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
      Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
    LOGGER.debug("{} {} write initTag {} comSeqNum {} TargetTransTag {} DataDeqNum {} index {} length {}", address, port,
        initiatorTaskTag, commandSequenceNumber, targetTransferTag, dataSequenceNumber, storageIndex, bytes.length);
    try {
      write(bytes, storageIndex);
    } catch (IOException e) {
      LOGGER.error("Unknown error", e);
      throw e;
    }
  }

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
    LOGGER.debug("close");
    _store.close();
  }

  @Override
  public void flushWrites() throws IOException {
    LOGGER.debug("flush");
    _store.fsync();
  }

}
