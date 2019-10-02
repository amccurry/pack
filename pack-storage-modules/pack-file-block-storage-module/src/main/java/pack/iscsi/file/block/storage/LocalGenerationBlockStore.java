package pack.iscsi.file.block.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.BlockKey;
import pack.iscsi.spi.block.BlockGenerationStore;

public class LocalGenerationBlockStore implements BlockGenerationStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalGenerationBlockStore.class);

  private final RandomAccessFile _randomAccessFile;
  private final FileChannel _channel;

  public LocalGenerationBlockStore(File blockStoreFile, int blockSize, long lengthInBytes) throws IOException {
    _randomAccessFile = new RandomAccessFile(blockStoreFile, "rw");
    long length = ((lengthInBytes / blockSize) + 1) * 8;
    _randomAccessFile.setLength(length);
    _channel = _randomAccessFile.getChannel();
  }

  @Override
  public Map<BlockKey, Long> getAllLastStoredGeneration(long volumeId) throws IOException {
    int count = (int) (_randomAccessFile.length() / 8);
    long position = 0;
    ByteBuffer buffer = ByteBuffer.allocate(8);
    Map<BlockKey, Long> results = new HashMap<>();
    for (int i = 0; i < count; i++) {
      buffer.reset();
      while (buffer.hasRemaining()) {
        position += _channel.read(buffer, position);
      }
      buffer.flip();
      results.put(BlockKey.builder()
                          .volumeId(0)
                          .blockId(i)
                          .build(),
          buffer.getLong());
    }
    return results;
  }

  @Override
  public long getLastStoredGeneration(long volumeId, long blockId) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    long position = blockId * 8;
    while (buffer.remaining() > 0) {
      position += _channel.read(buffer, position);
    }
    buffer.flip();
    return buffer.getLong();
  }

  @Override
  public void setLastStoredGeneration(long volumeId, long blockId, long lastStoredGeneration) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(lastStoredGeneration);
    buffer.flip();
    long position = blockId * 8;
    while (buffer.remaining() > 0) {
      position += _channel.write(buffer, position);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(LOGGER, _channel, _randomAccessFile);
  }

}
