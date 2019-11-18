package pack.iscsi.volume.cache;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.RandomAccessIOReader;
import pack.iscsi.volume.BlockStorageModuleConfig;

public class MultiLocalCacheFileFactory implements LocalFileCacheFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiLocalCacheFileFactory.class);
  private static final String RW = "rw";

  private final int _blockSize;
  private final long _volumeId;
  private final File[] _blockDataDirs;
  private final File[] _volumeDirs;
  private final Random _random = new Random();

  public MultiLocalCacheFileFactory(BlockStorageModuleConfig config) throws IOException {
    _volumeId = config.getVolumeId();
    _blockSize = config.getBlockSize();
    _blockDataDirs = config.getBlockDataDirs();
    IOUtils.mkdirs(_blockDataDirs);

    _volumeDirs = new File[_blockDataDirs.length];
    int index = 0;
    for (File blockDataDir : _blockDataDirs) {
      File volumeDir = new File(blockDataDir, Long.toString(_volumeId));
      volumeDir.mkdirs();
      _volumeDirs[index++] = volumeDir;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.rmr(_volumeDirs);
  }

  @Override
  public RandomAccessIO getRandomAccessIO(long volumeId, long blockId) throws IOException {
    File file = new File(getVolumeDir(), Long.toString(blockId));
    RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, _blockSize, RW);
    randomAccessIO.setLength(_blockSize);
    return new RandomAccessIO() {

      @Override
      public void read(long position, byte[] buffer, int offset, int length) throws IOException {
        randomAccessIO.read(position, buffer, offset, length);
      }

      @Override
      public long length() throws IOException {
        return randomAccessIO.length();
      }

      @Override
      public void write(long position, byte[] buffer, int offset, int length) throws IOException {
        randomAccessIO.write(position, buffer, offset, length);
      }

      @Override
      public void setLength(long length) throws IOException {
        randomAccessIO.setLength(length);
      }

      @Override
      public void close() throws IOException {
        randomAccessIO.close();
        LOGGER.info("Removing local cache file {}", file);
        file.delete();
      }

      @Override
      public RandomAccessIOReader cloneReadOnly() throws IOException {
        return randomAccessIO.cloneReadOnly();
      }
    };
  }

  private synchronized File getVolumeDir() {
    return _volumeDirs[_random.nextInt(_volumeDirs.length)];
  }

}
