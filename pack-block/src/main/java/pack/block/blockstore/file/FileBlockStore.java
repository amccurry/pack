package pack.block.blockstore.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.BlockStore;
import pack.block.server.fs.Ext4LinuxFileSystem;
import pack.block.server.fs.LinuxFileSystem;
import pack.block.util.Utils;

public class FileBlockStore implements BlockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBlockStore.class);
  private static final String RW = "rw";

  private final File _file;
  private final RandomAccessFile _rand;
  private final FileChannel _channel;
  private final long _length;
  private final boolean _enableSync;

  public FileBlockStore(File file, long length, boolean enableSync) throws IOException {
    file.getParentFile()
        .mkdirs();
    _enableSync = enableSync;
    boolean exists = file.exists();
    _file = file;
    _length = file.length();
    _rand = new RandomAccessFile(_file, RW);
    if (!exists) {
      _rand.setLength(length);
    }
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
    return _channel.write(ByteBuffer.wrap(buffer, offset, len), position);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    return _channel.read(ByteBuffer.wrap(buffer, offset, len), position);
  }

  @Override
  public void fsync() throws IOException {
    if (_enableSync) {
      _channel.force(true);
    }
  }

  @Override
  public LinuxFileSystem getLinuxFileSystem() {
    return Ext4LinuxFileSystem.INSTANCE;
  }

  @Override
  public void delete(long position, long length) throws IOException {
    Utils.punchHole(LOGGER, _file, position, length);
  }

  public long getNumberOfBlocksOnDisk() throws IOException {
    return Utils.getNumberOfBlocksOnDisk(LOGGER, _file);
  }

}
