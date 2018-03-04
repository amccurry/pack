package pack.file.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.BaseStorageModule;

public class FileStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageModule.class);

  private RandomAccessFile rand;
  private Object lock = new Object();

  public FileStorageModule(long sizeInBytes, int blockSize, String name, File file) throws IOException {
    super(sizeInBytes, blockSize, name);
    rand = new RandomAccessFile(file, "rw");
  }

  @Override
  public void read(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("read {} {}", bytes.length, storageIndex);
    synchronized (lock) {
      rand.seek(storageIndex);
      rand.read(bytes);
    }
  }

  @Override
  public void write(byte[] bytes, long storageIndex) throws IOException {
    LOGGER.info("write {} {}", bytes.length, storageIndex);
    synchronized (lock) {
      rand.seek(storageIndex);
      rand.write(bytes);
    }
  }

  @Override
  public void close() throws IOException {
    rand.close();
  }

}
