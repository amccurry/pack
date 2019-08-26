package pack.iscsi.partitioned.block;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.util.IOUtils;

public class Block implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Block.class);

  private static final String RW = "rw";
  private static final int BLOCK_OVERHEAD = 9;

  private final File _blockDataFile;
  private final long _blockId;
  private final long _volumeId;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final int _blockSize;
  private final BlockGenerationStore _blockGenerationStore;
  private final BlockWriteAheadLog _wal;
  private final RandomAccessFile _raf;
  private final FileChannel _channel;
  private final AtomicReference<BlockState> _onDiskState = new AtomicReference<>();
  private final AtomicLong _onDiskGeneration = new AtomicLong();
  private final AtomicLong _lastStoredGeneration = new AtomicLong();
  private final AtomicBoolean _closed = new AtomicBoolean();

  public Block(File blockDataDir, long volumeId, long blockId, int blockSize, BlockGenerationStore blockGenerationStore,
      BlockWriteAheadLog wal) throws IOException {
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    _blockGenerationStore = blockGenerationStore;
    _wal = wal;
    _blockSize = blockSize;
    _volumeId = volumeId;
    _blockId = blockId;

    createAndCheckExistence(blockDataDir);

    File volumeDir = new File(blockDataDir, Long.toString(volumeId));
    createAndCheckExistence(volumeDir);

    _blockDataFile = new File(volumeDir, Long.toString(blockId));

    if (_blockDataFile.exists() && isLengthValid()) {
      // recovery will need to occur, may be out of date
      _raf = new RandomAccessFile(_blockDataFile, RW);
      _channel = _raf.getChannel();
    } else {
      if (_blockDataFile.exists()) {
        LOGGER.info("Block file {} length incorrect actual {} expecting {}, remove and recover.", _blockDataFile,
            _blockDataFile.length(), getValidLength());
        _blockDataFile.delete();
      }
      _raf = new RandomAccessFile(_blockDataFile, RW);
      _raf.setLength(_blockSize + BLOCK_OVERHEAD);
      _channel = _raf.getChannel();
    }
    readMetadata();
    long generation = blockGenerationStore.getGeneration(_volumeId, _blockId);
    _lastStoredGeneration.set(generation);
    
    // @TODO fetch wal generation and not allow reads until block is up to date?
    
  }

  /**
   * Position is relative to the block.
   */
  public void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    checkIfClosed();
    checkState();
    checkPositionAndLength(blockPosition, len);
    try {
      ByteBuffer dst = ByteBuffer.wrap(bytes, offset, len);
      while (dst.remaining() > 0) {
        blockPosition += _channel.read(dst, blockPosition);
      }
    } finally {
      _readLock.unlock();
    }
  }

  /**
   * Position is relative to the block.
   */
  public void writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _writeLock.lock();
    checkIfClosed();
    checkState();
    checkPositionAndLength(blockPosition, len);
    try {
      markDirty();
      long generation = _onDiskGeneration.incrementAndGet();
      _wal.write(_volumeId, _blockId, generation, bytes, offset, len);
      writeMetadata();
      _blockGenerationStore.updateGeneration(_volumeId, _blockId, generation);
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, len);
      while (src.remaining() > 0) {
        blockPosition += _channel.write(src, blockPosition);
      }
    } finally {
      _writeLock.unlock();
    }
  }

  private void checkPositionAndLength(long blockPosition, int len) throws EOFException {
    if (blockPosition + len > _blockSize) {
      throw new EOFException("volume " + _volumeId + " block " + _blockId + " blockPosition " + blockPosition
          + " length " + len + " read/write pass end of block");
    }
  }

  public void execIO(BlockIOExecutor executor) throws IOException {
    _writeLock.lock();
    try {
      checkIfClosed();
      BlockIOResult result = executor.exec(_channel, _blockDataFile, _blockSize, _onDiskGeneration.get(),
          _onDiskState.get(), _lastStoredGeneration.get());
      _onDiskState.set(result.getOnDiskBlockState());
      _onDiskGeneration.set(result.getOnDiskGeneration());
      _lastStoredGeneration.set(result.getLastStoredGeneration());
      writeMetadata();
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    _writeLock.lock();
    try {
      if (!_closed.get()) {
        _closed.set(true);
      }
      IOUtils.closeQuietly(_channel, _raf);
    } finally {
      _writeLock.unlock();
    }
  }

  private void markDirty() {
    _onDiskState.set(BlockState.DIRTY);
  }

  private void checkState() throws IOException {
    BlockState state = _onDiskState.get();
    if (state == null || state == BlockState.MISSING) {
      throw new IOException(
          "volume " + _volumeId + " block " + _blockId + " state " + state + " is invalid for reading/writing.");
    }
  }

  private void writeMetadata() throws IOException {
    ByteBuffer buffer = getMetadataBuffer();
    _channel.write(buffer, getMetadataPosition());
    LOGGER.info("write meta data blockId {} state {} on disk generation {} last stored generation {}", _blockId,
        _onDiskState, _onDiskGeneration, _lastStoredGeneration);
  }

  private void readMetadata() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_OVERHEAD);
    _channel.read(buffer, getMetadataPosition());
    buffer.flip();
    _onDiskState.set(BlockState.lookup(buffer.get()));
    _onDiskGeneration.set(buffer.getLong());
  }

  private ByteBuffer getMetadataBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_OVERHEAD);
    BlockState blockState = _onDiskState.get();
    buffer.put(blockState.getType());
    buffer.putLong(_onDiskGeneration.get());
    buffer.flip();
    return buffer;
  }

  private void checkIfClosed() throws IOException {
    if (_closed.get()) {
      throw new IOException("volume " + _volumeId + " block " + _blockId + " already closed");
    }
  }

  private long getMetadataPosition() {
    return _blockSize;
  }

  private long getValidLength() {
    return _blockSize + BLOCK_OVERHEAD;
  }

  private boolean isLengthValid() {
    return _blockDataFile.length() == getValidLength();
  }

  private void createAndCheckExistence(File dir) throws IOException {
    dir.mkdirs();
    if (!dir.exists()) {
      throw new IOException("Directory " + dir + " does not exist.");
    }
  }

  public long getBlockId() {
    return _blockId;
  }

  public long getVolumeId() {
    return _volumeId;
  }

  public BlockState getOnDiskState() {
    return _onDiskState.get();
  }

  public long getOnDiskGeneration() {
    return _onDiskGeneration.get();
  }

  public long getLastStoredGeneration() {
    return _lastStoredGeneration.get();
  }
}
