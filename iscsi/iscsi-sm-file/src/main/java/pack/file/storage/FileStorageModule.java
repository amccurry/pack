package pack.file.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.BaseStorageModule;

public class FileStorageModule extends BaseStorageModule {

  private static final String RW = "rw";

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageModule.class);

  private final RandomAccessFile _rand;
  private final Object _lock = new Object();

  public FileStorageModule(long sizeInBytes, int blockSize, String name, File file) throws IOException {
    super(sizeInBytes, blockSize, name);
    _rand = new RandomAccessFile(file, RW);
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("read boff {} len {} bid {}  pos {}", getBlockOffset(storageIndex), bytes.length,
        getBlockId(storageIndex), storageIndex);
    synchronized (_lock) {
      _rand.seek(storageIndex);
      _rand.read(bytes);
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("write boff {} len {} bid {}  pos {}", getBlockOffset(storageIndex), bytes.length,
        getBlockId(storageIndex), storageIndex);
    synchronized (_lock) {
      _rand.seek(storageIndex);
      _rand.write(bytes);
    }
  }

  @Override
  public void close() throws IOException {
    _rand.close();
  }

}
