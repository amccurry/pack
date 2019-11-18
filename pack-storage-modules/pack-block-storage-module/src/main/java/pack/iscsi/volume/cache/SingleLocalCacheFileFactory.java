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

public class SingleLocalCacheFileFactory implements LocalFileCacheFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleLocalCacheFileFactory.class);

  private static final String RW = "rw";

  private final RandomAccessIO _randomAccessIO;
  private final int _blockSize;
  private final File _file;

  public SingleLocalCacheFileFactory(BlockStorageModuleConfig config) throws IOException {
    File[] blockDataDirs = config.getBlockDataDirs();
    IOUtils.mkdirs(blockDataDirs);
    Random random = new Random();
    File blockDataDir = blockDataDirs[random.nextInt(blockDataDirs.length)];
    _file = new File(blockDataDir, Long.toString(config.getVolumeId()));
    _randomAccessIO = FileIO.openRandomAccess(_file, config.getBlockSize(), RW);
    _blockSize = config.getBlockSize();
    _randomAccessIO.setLength(config.getBlockCount() * _blockSize);
  }

  @Override
  public void close() throws IOException {
    _randomAccessIO.close();
    _file.delete();
  }

  @Override
  public RandomAccessIO getRandomAccessIO(long volumeId, long blockId) {
    long blockOffset = (long) _blockSize * blockId;
    return new RandomAccessIO() {

      @Override
      public void read(long position, byte[] buffer, int offset, int length) throws IOException {
        _randomAccessIO.read(position + blockOffset, buffer, offset, length);
      }

      @Override
      public long length() throws IOException {
        return _randomAccessIO.length();
      }

      @Override
      public void write(long position, byte[] buffer, int offset, int length) throws IOException {
        _randomAccessIO.write(position + blockOffset, buffer, offset, length);
      }

      @Override
      public void setLength(long length) throws IOException {
        _randomAccessIO.setLength(length);
      }

      @Override
      public void close() throws IOException {
        LOGGER.info("punching hole in volume id {} for block id {}", volumeId, blockId);
        _randomAccessIO.punchHole(blockOffset, _blockSize);
      }

      @Override
      public RandomAccessIOReader cloneReadOnly() throws IOException {
        RandomAccessIOReader cloneReadOnly = _randomAccessIO.cloneReadOnly();
        return new RandomAccessIOReader() {

          @Override
          public void close() throws IOException {
            cloneReadOnly.close();
          }

          @Override
          public void read(long position, byte[] buffer, int offset, int length) throws IOException {
            cloneReadOnly.read(position + blockOffset, buffer, offset, length);
          }

          @Override
          public long length() throws IOException {
            return cloneReadOnly.length();
          }
        };
      }
    };
  }

}
