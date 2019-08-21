package pack.iscsi.s3;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

import pack.iscsi.spi.BaseStorageModule;
import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;
import pack.util.IOUtils;

public class S3StorageModule extends BaseStorageModule {

  public static class S3StorageModuleFactory implements StorageModuleFactory {

    private final File _volumeDir;

    public S3StorageModuleFactory(File volumeDir) {
      _volumeDir = volumeDir;
      _volumeDir.mkdirs();
    }

    @Override
    public List<String> getStorageModuleNames() {
      return Arrays.asList(_volumeDir.list());
    }

    @Override
    public StorageModule getStorageModule(String name) throws IOException {
      return new S3StorageModule(new File(_volumeDir, name));
    }

  }

  public static S3StorageModuleFactory createFactory(File volumeDir) {
    return new S3StorageModuleFactory(volumeDir);
  }

  private final RandomAccessFile _raf;
  private final FileChannel _channel;

  public S3StorageModule(File volumeFile) throws IOException {
    super(volumeFile.length());
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
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    while (buffer.remaining() > 0) {
      position += _channel.write(buffer, position);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(_channel, _raf);
  }

}
