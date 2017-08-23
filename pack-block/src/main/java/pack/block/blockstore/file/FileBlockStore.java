package pack.block.blockstore.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import pack.block.blockstore.BlockStore;
import pack.block.server.fs.Ext4LinuxFileSystem;
import pack.block.server.fs.LinuxFileSystem;

public class FileBlockStore implements BlockStore {

  private final File _file;
  private final RandomAccessFile _rand;
  private final FileChannel _channel;
  private final long _length;

  public FileBlockStore(File file) throws IOException {
    _file = file;
    _length = file.length();
    _rand = new RandomAccessFile(_file, "rw");
    _channel = _rand.getChannel();
  }

  @Override
  public void close() throws IOException {
    _channel.close();
    _rand.close();
  }

  @Override
  public String getName() {
    return _file.getName();
  }

  @Override
  public long getLength() {
    return _length;
  }

  @Override
  public long lastModified() {
    return _file.lastModified();
  }

  @Override
  public int write(long position, byte[] buffer, int offset, int len) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
    return _channel.write(byteBuffer, position);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
    return _channel.read(byteBuffer, position);
  }

  @Override
  public void fsync() throws IOException {
    _channel.force(true);
  }

  @Override
  public LinuxFileSystem getLinuxFileSystem() {
    return Ext4LinuxFileSystem.INSTANCE;
  }

}
