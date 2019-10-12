package pack.iscsi.block;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.block.BlockMetadata;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;

public class LocalBlockStateStore implements BlockStateStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlockStateStore.class);

  private static final int BLOCK_METADATA_LENGTH = 9;

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
    private final AtomicLong _maxBlockCount = new AtomicLong();

    FileRef(File file) throws IOException {
      _file = file;
      _file.getParentFile()
           .mkdirs();
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
  public synchronized BlockMetadata getBlockMetadata(long volumeId, long blockId) throws IOException {
    LOGGER.debug("getBlockMetadata volumeId {} blockId {}", volumeId, blockId);
    FileRef fileRef = getFileRef(volumeId);
    checkFileRef(volumeId, fileRef);
    checkBlockId(volumeId, blockId, fileRef);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_METADATA_LENGTH);
    readFully(fileRef, position, buffer);
    buffer.flip();
    return BlockMetadata.builder()
                        .blockState(BlockState.lookup(buffer.get()))
                        .generation(buffer.getLong())
                        .build();
  }

  private void checkFileRef(long volumeId, FileRef fileRef) throws IOException {
    if (fileRef == null) {
      throw new IOException("Block state store not open for volumeId " + volumeId);
    }
  }

  private void checkBlockId(long volumeId, long blockId, FileRef fileRef) throws IOException {
    if (blockId >= fileRef._maxBlockCount.get()) {
      throw new IOException(
          "blockId " + blockId + " for " + volumeId + " too large max blocks " + fileRef._maxBlockCount);
    }
  }

  @Override
  public synchronized void setBlockMetadata(long volumeId, long blockId, BlockMetadata metadata) throws IOException {
    LOGGER.debug("setBlockMetadata volumeId {} blockId {} metadata {}", volumeId, blockId, metadata);
    FileRef fileRef = getFileRef(volumeId);
    checkFileRef(volumeId, fileRef);
    checkBlockId(volumeId, blockId, fileRef);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_METADATA_LENGTH)
                                  .put(metadata.getBlockState()
                                               .getType())
                                  .putLong(metadata.getGeneration());
    buffer.flip();
    writeFully(fileRef, position, buffer);
  }

  @Override
  public synchronized void removeBlockMetadata(long volumeId, long blockId) throws IOException {
    FileRef fileRef = getFileRef(volumeId);
    checkFileRef(volumeId, fileRef);
    checkBlockId(volumeId, blockId, fileRef);
    long position = getPosition(blockId);
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_METADATA_LENGTH);
    writeFully(fileRef, position, buffer);
  }

  @Override
  public synchronized void setMaxBlockCount(long volumeId, long blockCount) throws IOException {
    long length = getPosition(blockCount + 1);
    FileRef fileRef = getFileRef(volumeId);
    fileRef._maxBlockCount.set(blockCount);
    long currentLength = fileRef._raf.length();
    fileRef._raf.setLength(length);
    zeroOut(fileRef._channel, currentLength, length - currentLength);
  }

  private void zeroOut(FileChannel channel, long position, long length) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
    while (length > 0) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      duplicate.limit((int) Math.min(length, duplicate.capacity()));
      duplicate.position(0);
      int write = channel.write(duplicate, position);
      position += write;
      length -= write;
    }
  }

  @Override
  public synchronized void createBlockMetadataStore(long volumeId) throws IOException {
    FileRef fileRef = getFileRef(volumeId);
    if (fileRef != null) {
      LOGGER.warn("BlockMetadataStore already created for {}", volumeId);
      return;
    }
    File file = new File(_blockStateDir, Long.toString(volumeId));
    fileRef = new FileRef(file);
    _metadataMap.put(volumeId, fileRef);
  }

  private FileRef getFileRef(long volumeId) {
    return _metadataMap.get(volumeId);
  }

  @Override
  public synchronized void destroyBlockMetadataStore(long volumeId) throws IOException {
    FileRef fileRef = _metadataMap.remove(volumeId);
    IOUtils.close(LOGGER, fileRef);
    if (fileRef != null) {
      fileRef._file.delete();
    }
  }

  private long getPosition(long blockId) {
    return blockId * (long) BLOCK_METADATA_LENGTH;
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
