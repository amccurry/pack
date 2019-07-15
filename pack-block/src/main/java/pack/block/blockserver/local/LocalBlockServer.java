package pack.block.blockserver.local;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import pack.block.blockserver.BlockServer;
import pack.block.blockserver.BlockServerException;

public class LocalBlockServer implements BlockServer {

  private static final String RW = "rw";
  private final File _root;
  private final Map<Long, Handle> _randMap = new ConcurrentHashMap<>();
  private final AtomicInteger _handleCount = new AtomicInteger(1);

  static class Handle implements Closeable {
    final RandomAccessFile _rand;
    final FileChannel _channel;

    public Handle(RandomAccessFile rand, FileChannel channel) {
      _rand = rand;
      _channel = channel;
    }

    @Override
    public void close() throws IOException {
      _channel.close();
      _rand.close();
    }
  }

  public LocalBlockServer(File root) {
    _root = root;
  }

  @Override
  public byte[] read(long mountId, long position, int length) throws IOException {
    byte[] buf = new byte[length];
    Handle handle = _randMap.get(mountId);
    ByteBuffer bb = ByteBuffer.wrap(buf);
    while (bb.remaining() > 0) {
      position += handle._channel.read(bb, position);
    }
    return buf;
  }

  @Override
  public void write(long mountId, long position, byte[] data) throws IOException {
    Handle handle = _randMap.get(mountId);
    ByteBuffer bb = ByteBuffer.wrap(data);
    while (bb.remaining() > 0) {
      position += handle._channel.write(bb, position);
    }
  }

  @Override
  public void delete(long mountId, long position, int length) throws IOException {

  }

  @Override
  public long mount(String volumeName, String snapshotId) throws IOException {
    File file = new File(new File(_root, volumeName), snapshotId);
    RandomAccessFile rand = new RandomAccessFile(file, RW);
    long mountId = _handleCount.incrementAndGet();
    _randMap.put(mountId, new Handle(rand, rand.getChannel()));
    return mountId;
  }

  @Override
  public void unmount(long mountId) throws IOException {
    try (Handle handle = _randMap.remove(mountId)) {

    }
  }

  @Override
  public List<String> volumes() throws IOException {
    return getDirectoryListing(_root, (FileFilter) pathname -> pathname.isDirectory());
  }

  @Override
  public List<String> snapshots(String volumeName) throws IOException, BlockServerException {
    File volumeDir = new File(_root, volumeName);
    if (!volumeDir.exists()) {
      throw new BlockServerException("Volume " + volumeName + " does not exist");
    }
    return getDirectoryListing(volumeDir, (FileFilter) pathname -> pathname.isFile());
  }

  private List<String> getDirectoryListing(File dir, FileFilter fileFilter) {
    List<String> result = new ArrayList<>();
    File[] listFiles = dir.listFiles(fileFilter);
    for (File f : listFiles) {
      result.add(f.getName());
    }
    return result;
  }

  @Override
  public long getSnapshotLength(String volumeName, String snapshotId) throws IOException, BlockServerException {
    File file = new File(new File(_root, volumeName), snapshotId);
    return file.length();
  }
}
