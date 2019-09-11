package pack.iscsi.block;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;
import pack.iscsi.volume.BlockGenerationStore;
import pack.iscsi.volume.VolumeMetadata;
import pack.util.IOUtils;

public class LocalBlock implements Closeable, Block {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlock.class);

  private static final String RW = "rw";
  private static final int BLOCK_OVERHEAD = 9;

  private final long _blockId;
  private final long _volumeId;
  private final int _blockSize;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final File _blockDataFile;
  private final RandomAccessFile _raf;
  private final FileChannel _channel;
  private final AtomicReference<BlockState> _onDiskState = new AtomicReference<>();
  private final AtomicLong _onDiskGeneration = new AtomicLong();
  private final AtomicLong _lastStoredGeneration = new AtomicLong();
  private final AtomicLong _lastWrite = new AtomicLong();
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final BlockWriteAheadLog _wal;
  private final BlockGenerationStore _blockStore;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;

  public LocalBlock(LocalBlockConfig config) throws IOException {
    VolumeMetadata volumeMetadata = config.getVolumeMetadata();
    _volumeId = volumeMetadata.getVolumeId();
    _blockId = config.getBlockId();
    File blockDataDir = config.getBlockDataDir();
    createAndCheckExistence(blockDataDir);

    File volumeDir = new File(blockDataDir, Long.toString(_volumeId));
    createAndCheckExistence(volumeDir);

    _blockDataFile = new File(volumeDir, Long.toString(_blockId));

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    _blockStore = config.getBlockStore();

    _wal = config.getWal();
    _blockSize = volumeMetadata.getBlockSize();

    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();

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
    long lastStoreGeneration = _blockStore.getLastStoreGeneration(_volumeId, _blockId);
    LOGGER.info("volumeId {} blockId {} last store generation {} on disk generation {}", _volumeId, _blockId,
        lastStoreGeneration, _onDiskGeneration.get());
    _lastStoredGeneration.set(lastStoreGeneration);
  }

  @Override
  public void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _readLock.lock();
    checkIfClosed();
    checkState();
    checkPositionAndLength(blockPosition, len);
    checkGenerations();
    try {
      ByteBuffer dst = ByteBuffer.wrap(bytes, offset, len);
      while (dst.remaining() > 0) {
        blockPosition += _channel.read(dst, blockPosition);
      }
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public BlockWriteAheadLogResult writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _writeLock.lock();
    checkIfClosed();
    checkState();
    checkPositionAndLength(blockPosition, len);
    checkGenerations();
    try {
      _lastWrite.set(System.nanoTime());
      markDirty();
      long generation = _onDiskGeneration.incrementAndGet();
      BlockWriteAheadLogResult result = _wal.write(_volumeId, _blockId, generation, blockPosition, bytes, offset, len);
      writeMetadata();
      ByteBuffer src = ByteBuffer.wrap(bytes, offset, len);
      while (src.remaining() > 0) {
        blockPosition += _channel.write(src, blockPosition);
      }
      return result;
    } finally {
      _writeLock.unlock();
    }
  }

  private void checkPositionAndLength(long blockPosition, int len) throws EOFException {
    if (blockPosition + len > _blockSize) {
      throw new EOFException("volumeId " + _volumeId + " blockId " + _blockId + " blockPosition " + blockPosition
          + " length " + len + " read/write pass end of block");
    }
  }

  @Override
  public void execIO(BlockIOExecutor executor) throws IOException {
    _writeLock.lock();
    try {
      checkIfClosed();
      BlockIORequest request = BlockIORequest.builder()
                                             .blockId(_blockId)
                                             .blockSize(_blockSize)
                                             .channel(_channel)
                                             .lastStoredGeneration(_lastStoredGeneration.get())
                                             .onDiskGeneration(_onDiskGeneration.get())
                                             .onDiskState(_onDiskState.get())
                                             .volumeId(_volumeId)
                                             .build();
      BlockIOResponse response = executor.exec(request);
      _onDiskState.set(response.getOnDiskBlockState());
      _onDiskGeneration.set(response.getOnDiskGeneration());
      _lastStoredGeneration.set(response.getLastStoredGeneration());
      _blockStore.setLastStoreGeneration(_volumeId, _blockId, response.getLastStoredGeneration());
      _wal.release(_volumeId, _blockId, response.getLastStoredGeneration());
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

  @Override
  public void cleanUp() throws IOException {
    _writeLock.lock();
    try {
      if (!_closed.get()) {
        throw new IOException("Clean up can only occur after a close");
      }
      _blockDataFile.delete();
    } finally {
      _writeLock.unlock();
    }
  }

  private void markDirty() {
    _onDiskState.set(BlockState.DIRTY);
  }

  private void checkState() throws IOException {
    BlockState state = _onDiskState.get();
    if (state == null || state == BlockState.UNKNOWN) {
      throw new IOException(
          "volumeId " + _volumeId + " blockId " + _blockId + " state " + state + " is invalid for reading/writing.");
    }
  }

  private void writeMetadata() throws IOException {
    ByteBuffer buffer = getMetadataBuffer();
    _channel.write(buffer, getMetadataPosition());
    LOGGER.debug("write meta data volumeId {} blockId {} state {} on disk generation {} last stored generation {}",
        _volumeId, _blockId, _onDiskState, _onDiskGeneration, _lastStoredGeneration);
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
      throw new AlreadyClosedException("volumeId " + _volumeId + " blockId " + _blockId + " already closed");
    }
  }

  private void checkGenerations() throws IOException {
    if (_lastStoredGeneration.get() > _onDiskGeneration.get()) {
      throw new IOException("Last generation " + _lastStoredGeneration + " should never be ahead of on disk generation "
          + _onDiskGeneration);
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

  @Override
  public long getBlockId() {
    return _blockId;
  }

  @Override
  public long getVolumeId() {
    return _volumeId;
  }

  @Override
  public BlockState getOnDiskState() {
    return _onDiskState.get();
  }

  @Override
  public long getOnDiskGeneration() {
    return _onDiskGeneration.get();
  }

  @Override
  public long getLastStoredGeneration() {
    return _lastStoredGeneration.get();
  }

  @Override
  public int getSize() {
    return (int) getValidLength();
  }

  @Override
  public boolean isClosed() {
    return _closed.get();
  }

  @Override
  public boolean idleWrites() {
    long syncTimeAfterIdle = _syncTimeAfterIdleTimeUnit.toNanos(_syncTimeAfterIdle);
    return _lastWrite.get() + syncTimeAfterIdle < System.nanoTime();
  }

}
