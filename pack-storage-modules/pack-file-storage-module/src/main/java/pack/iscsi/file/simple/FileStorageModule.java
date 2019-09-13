package pack.iscsi.file.simple;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.spi.BaseStorageModule;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.util.IOUtils;

public class FileStorageModule extends BaseStorageModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageModule.class);

  public static class FileStorageModuleFactory implements StorageModuleFactory {

    private final File _volumeDir;

    public FileStorageModuleFactory(File volumeDir) {
      _volumeDir = volumeDir;
      _volumeDir.mkdirs();
    }

    @Override
    public List<String> getStorageModuleNames() {
      return Arrays.asList(_volumeDir.list());
    }

    @Override
    public StorageModule getStorageModule(String name) throws IOException {
      return new FileStorageModule(new File(_volumeDir, name));
    }

  }

  public static FileStorageModuleFactory createFactory(File volumeDir) {
    return new FileStorageModuleFactory(volumeDir);
  }

  private final RandomAccessFile _raf;
  private final FileChannel _channel;
  private final File _volumeFile;
  private final AtomicLong _writes = new AtomicLong();
  private final AtomicLong _writeEvents = new AtomicLong();

  public FileStorageModule(File volumeFile) throws IOException {
    super(volumeFile.length());
    _volumeFile = volumeFile;
    LOGGER.info("Creating {}", volumeFile);
    _raf = new RandomAccessFile(volumeFile, "rw");
    _channel = _raf.getChannel();
  }

  @Override
  public void read(byte[] bytes, long position) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    while (buffer.remaining() > 0) {
      position += _channel.read(buffer, position);
    }
  }

  @Override
  public void write(byte[] bytes, long position) throws IOException {
    _writes.addAndGet(bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    while (buffer.remaining() > 0) {
      position += _channel.write(buffer, position);
    }
  }

  @Override
  public void flushWrites() throws IOException {
    _writeEvents.incrementAndGet();
    LOGGER.info("{} buffered writes {}", _writeEvents.get(), _writes.getAndSet(0));
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_channel, _raf);
    LOGGER.info("Closing {}", _volumeFile);
  }

  @Override
  public int getBlockSize() {
    return 4096;
  }

}
