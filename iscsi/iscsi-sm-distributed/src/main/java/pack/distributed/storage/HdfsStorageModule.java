package pack.distributed.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.BaseStorageModule;

public class HdfsStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsStorageModule.class);

  public HdfsStorageModule(String name, HdfsMetaData hdfsMetaData, Configuration conf, Path volumeDir) {
    super(hdfsMetaData.getLength(), hdfsMetaData.getBlockSize(), name);
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    int blockOffset = getBlockOffset(storageIndex);
    int blockId = getBlockId(storageIndex);

    LOGGER.info("read boff {} len {} bid {}  pos {}", blockOffset, bytes.length, blockId, storageIndex);
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    int blockOffset = getBlockOffset(storageIndex);
    int blockId = getBlockId(storageIndex);

    LOGGER.info("write boff {} len {} bid {}  pos {}", blockOffset, bytes.length, blockId, storageIndex);
  }

  @Override
  public void flushWrites() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

}
