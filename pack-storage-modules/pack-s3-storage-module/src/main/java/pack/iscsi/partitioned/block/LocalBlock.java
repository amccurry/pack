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

import pack.iscsi.partitioned.storagemanager.BlockStore;
import pack.iscsi.partitioned.storagemanager.BlockWriteAheadLog;
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
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final BlockStore _blockStore;
  private final BlockWriteAheadLog _wal;

  public LocalBlock(File blockDataDir, long volumeId, long blockId, int blockSize, BlockStore blockStore,
      BlockWriteAheadLog wal) throws IOException {
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();
    _blockStore = blockStore;
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
    long generation = blockStore.getGeneration(_volumeId, _blockId);
    _lastStoredGeneration.set(generation);
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
  public void writeFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    _writeLock.lock();
    checkIfClosed();
    checkState();
    checkPositionAndLength(blockPosition, len);
    checkGenerations();
    try {
      markDirty();
      long generation = _onDiskGeneration.incrementAndGet();
      _wal.write(_volumeId, _blockId, generation, bytes, offset, len);
      writeMetadata();
      _blockStore.updateGeneration(_volumeId, _blockId, generation);
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
                                             .fileForReadingOnly(_blockDataFile)
                                             .lastStoredGeneration(_lastStoredGeneration.get())
                                             .onDiskGeneration(_onDiskGeneration.get())
                                             .onDiskState(_onDiskState.get())
                                             .volumeId(_volumeId)
                                             .build();
      BlockIOResponse response = executor.exec(request);
      _onDiskState.set(response.getOnDiskBlockState());
      _onDiskGeneration.set(response.getOnDiskGeneration());
      _lastStoredGeneration.set(response.getLastStoredGeneration());
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
      throw new IOException("volumeId " + _volumeId + " blockId " + _blockId + " already closed");
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

}
