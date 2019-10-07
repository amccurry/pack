package pack.iscsi.block;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.block.Block;
import pack.iscsi.spi.block.BlockGenerationStore;
import pack.iscsi.spi.block.BlockIOExecutor;
import pack.iscsi.spi.block.BlockIORequest;
import pack.iscsi.spi.block.BlockIOResponse;
import pack.iscsi.spi.block.BlockMetadata;
import pack.iscsi.spi.block.BlockState;
import pack.iscsi.spi.block.BlockStateStore;
import pack.iscsi.spi.wal.BlockWriteAheadLog;
import pack.util.LockUtil;
import pack.util.tracer.TracerUtil;

public class LocalBlock implements Closeable, Block {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalBlock.class);

  private final long _blockId;
  private final long _volumeId;
  private final int _blockSize;
  private final WriteLock _writeLock;
  private final ReadLock _readLock;
  private final AtomicReference<BlockState> _onDiskState = new AtomicReference<>();
  private final AtomicLong _onDiskGeneration = new AtomicLong();
  private final AtomicLong _lastStoredGeneration = new AtomicLong();
  private final AtomicLong _lastWrite = new AtomicLong();
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final BlockWriteAheadLog _wal;
  private final BlockGenerationStore _blockStore;
  private final long _syncTimeAfterIdle;
  private final TimeUnit _syncTimeAfterIdleTimeUnit;
  private final RandomAccessIO _randomAccessIO;
  private final BlockStateStore _blockStateStore;
  private final long _startingPositionOfBlock;
  private final Executor _executor;

  public LocalBlock(LocalBlockConfig config) throws IOException {
    _executor = config.getExecutor();
    _randomAccessIO = config.getRandomAccessIO();
    _blockStore = config.getBlockGenerationStore();
    _wal = config.getWal();
    _volumeId = config.getVolumeId();
    _blockId = config.getBlockId();
    _blockStateStore = config.getBlockStateStore();
    _blockSize = config.getBlockSize();
    _syncTimeAfterIdle = config.getSyncTimeAfterIdle();
    _syncTimeAfterIdleTimeUnit = config.getSyncTimeAfterIdleTimeUnit();
    _startingPositionOfBlock = _blockId * _blockSize;

    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
    _writeLock = reentrantReadWriteLock.writeLock();
    _readLock = reentrantReadWriteLock.readLock();

    readMetadata();
    long lastStoreGeneration;
    try (Scope scope = TracerUtil.trace(LocalBlock.class, "last store generation")) {
      lastStoreGeneration = _blockStore.getLastStoredGeneration(_volumeId, _blockId);
    }
    LOGGER.debug("volumeId {} blockId {} last store generation {} on disk generation {}", _volumeId, _blockId,
        lastStoreGeneration, _onDiskGeneration.get());
    _lastStoredGeneration.set(lastStoreGeneration);
  }

  @Override
  public void readFully(long blockPosition, byte[] bytes, int offset, int len) throws IOException {
    try (Closeable lock = LockUtil.getCloseableLock(_readLock)) {
      checkIfClosed();
      checkState();
      checkPositionAndLength(blockPosition, len);
      checkGenerations();
      try (Scope scope = TracerUtil.trace(LocalBlock.class, "randomaccessio read")) {
        _randomAccessIO.readFully(getFilePosition(blockPosition), bytes, offset, len);
      }
    }
  }

  @Override
  public AsyncCompletableFuture writeFully(long blockPosition, byte[] bytes, int offset, int len, boolean autoFlush)
      throws IOException {
    try (Closeable lock = LockUtil.getCloseableLock(_writeLock)) {
      checkIfClosed();
      checkState();
      checkPositionAndLength(blockPosition, len);
      checkGenerations();
      _lastWrite.set(System.nanoTime());
      try (Scope scope = TracerUtil.trace(LocalBlock.class, "mark dirty")) {
        markDirty();
      }
      long generation = _onDiskGeneration.incrementAndGet();
      AsyncCompletableFuture comWalWrite;
      try (Scope scope = TracerUtil.trace(LocalBlock.class, "wal write")) {
        comWalWrite = _wal.write(_volumeId, _blockId, generation, blockPosition, bytes, offset, len);
      }
      writeMetadata();
      try (Scope scope = TracerUtil.trace(LocalBlock.class, "randomaccessio write")) {
        _randomAccessIO.writeFully(getFilePosition(blockPosition), bytes, offset, len);
      }
      if (autoFlush) {
        AsyncCompletableFuture comFlush = AsyncCompletableFuture.exec(getClass(), "flush", _executor,
            () -> _randomAccessIO.flush());
        return AsyncCompletableFuture.allOf(comFlush, comWalWrite);
      } else {
        return comWalWrite;
      }
    }
  }

  private long getFilePosition(long blockPosition) {
    return _startingPositionOfBlock + blockPosition;
  }

  @Override
  public void execIO(BlockIOExecutor executor) throws IOException {
    try (Closeable lock = LockUtil.getCloseableLock(_writeLock)) {
      checkIfClosed();
      BlockIORequest request = BlockIORequest.builder()
                                             .blockId(_blockId)
                                             .blockSize(_blockSize)
                                             .randomAccessIO(_randomAccessIO)
                                             .lastStoredGeneration(_lastStoredGeneration.get())
                                             .onDiskGeneration(_onDiskGeneration.get())
                                             .onDiskState(_onDiskState.get())
                                             .volumeId(_volumeId)
                                             .startingPositionOfBlock(_startingPositionOfBlock)
                                             .build();
      LOGGER.debug("starting execIO volumeId {} blockId {}", _volumeId, _blockId);
      BlockIOResponse response = executor.exec(request);
      LOGGER.debug("finished execIO volumeId {} blockId {}", _volumeId, _blockId);
      _onDiskState.set(response.getOnDiskBlockState());
      _onDiskGeneration.set(response.getOnDiskGeneration());
      _lastStoredGeneration.set(response.getLastStoredGeneration());
      LOGGER.debug("write last store generation volumeId {} blockId {}", _volumeId, _blockId);
      _blockStore.setLastStoredGeneration(_volumeId, _blockId, response.getLastStoredGeneration());
      LOGGER.debug("release journal volumeId {} blockId {} generation {}", _volumeId, _blockId,
          response.getLastStoredGeneration());
      _wal.releaseJournals(_volumeId, _blockId, response.getLastStoredGeneration());
      LOGGER.debug("write metadata {} blockId {}", _volumeId, _blockId);
      writeMetadata();
    }
  }

  @Override
  public void close() throws IOException {
    try (Closeable lock = LockUtil.getCloseableLock(_writeLock)) {
      if (!_closed.get()) {
        _closed.set(true);
      }

      _blockStateStore.removeBlockMetadata(_volumeId, _blockId);
      long position = _blockId * (long) _blockSize;
      LOGGER.info("punching hole in volume id {} for block id {}", _volumeId, _blockId);
      _randomAccessIO.punchHole(position, _blockSize);
    }
  }

  @Override
  public boolean idleWrites() {
    long syncTimeAfterIdle = _syncTimeAfterIdleTimeUnit.toNanos(_syncTimeAfterIdle);
    return _lastWrite.get() + syncTimeAfterIdle < System.nanoTime();
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
    try (Scope scope1 = TracerUtil.trace(LocalBlock.class, "metadata write")) {
      BlockMetadata metadata = BlockMetadata.builder()
                                            .blockState(_onDiskState.get())
                                            .generation(_onDiskGeneration.get())
                                            .build();
      _blockStateStore.setBlockMetadata(_volumeId, _blockId, metadata);
    }
  }

  private void readMetadata() throws IOException {
    try (Scope scope = TracerUtil.trace(LocalBlock.class, "metadata read")) {
      BlockMetadata blockMetadata = _blockStateStore.getBlockMetadata(_volumeId, _blockId);
      if (blockMetadata != null) {
        _onDiskState.set(blockMetadata.getBlockState());
        _onDiskGeneration.set(blockMetadata.getGeneration());
      } else {
        _onDiskState.set(BlockState.CLEAN);
        _onDiskGeneration.set(0);
      }
    }
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
    return _blockSize;
  }

  @Override
  public boolean isClosed() {
    return _closed.get();
  }

  private void checkPositionAndLength(long blockPosition, int len) throws EOFException {
    if (blockPosition + len > _blockSize) {
      throw new EOFException("volumeId " + _volumeId + " blockId " + _blockId + " blockPosition " + blockPosition
          + " length " + len + " read/write pass end of block with block size " + _blockSize);
    }
  }

}
