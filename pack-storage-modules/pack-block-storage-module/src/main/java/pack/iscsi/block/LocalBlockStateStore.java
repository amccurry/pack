package pack.iscsi.block;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.block.BlockMetadata;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;

public class LocalBlockStateStore implements BlockStateStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockStateStore.class);

  private static final long BLOCK_METADATA_LENGTH = 9;

  private final File _blockStateDir;
  private final Map<Long, FileRef> _metadataMap = new ConcurrentHashMap<>();

  public LocalBlockStateStore(LocalBlockStateStoreConfig config) {
    _blockStateDir = config.getBlockStateDir();
    _blockStateDir.mkdirs();
  }

  private static class FileRef implements Closeable {

    private final RandomAccessFile _raf;
    private final FileChannel _channel;
    private final File _file;

    FileRef(File file) throws IOException {
      _file = file;
      _raf = new RandomAccessFile(file, "rw");
      _channel = _raf.getChannel();
    }

    @Override
    public void close() throws IOException {
      IOUtils.close(LOGGER, _channel, _raf);
      _file.delete();
    }

  }

  @Override
  public BlockMetadata getBlockMetadata(long volumeId, long blockId) throws IOException {
    FileRef fileRef = _metadataMap.get(volumeId);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(9);
    readFully(fileRef, position, buffer);
    buffer.flip();
    return BlockMetadata.builder()
                        .blockState(BlockState.lookup(buffer.get()))
                        .generation(buffer.getLong())
                        .build();
  }

  @Override
  public void setBlockMetadata(long volumeId, long blockId, BlockMetadata metadata) throws IOException {
    FileRef fileRef = _metadataMap.get(volumeId);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(9)
                                  .put(metadata.getBlockState()
                                               .getType())
                                  .putLong(metadata.getGeneration());
    buffer.flip();
    writeFully(fileRef, position, buffer);
  }

  @Override
  public void removeBlockMetadata(long volumeId, long blockId) throws IOException {
    FileRef fileRef = _metadataMap.get(volumeId);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(9);
    writeFully(fileRef, position, buffer);
  }

  @Override
  public void setMaxBlockCount(long volumeId, long blockCount) throws IOException {
    long length = getPosition(blockCount);
    FileRef fileRef = _metadataMap.get(volumeId);
    fileRef._raf.setLength(length);
  }

  @Override
  public synchronized void createBlockMetadataStore(long volumeId) throws IOException {
    FileRef fileRef = _metadataMap.get(volumeId);
    if (fileRef != null) {
      LOGGER.warn("BlockMetadataStore already created for {}", volumeId);
      return;
    }
    File file = new File(_blockStateDir, Long.toString(volumeId));
    fileRef = new FileRef(file);
    _metadataMap.put(volumeId, fileRef);
  }

  @Override
  public void destroyBlockMetadataStore(long volumeId) throws IOException {
    IOUtils.close(LOGGER, _metadataMap.remove(volumeId));
  }

  private long getPosition(long blockId) {
    return blockId * BLOCK_METADATA_LENGTH;
  }

  private void writeFully(FileRef fileRef, long position, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      position += fileRef._channel.write(buffer, position);
    }
  }

  private void readFully(FileRef fileRef, long position, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      position += fileRef._channel.read(buffer, position);
    }
  }
}
