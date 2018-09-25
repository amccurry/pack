package pack.block.blockstore.hdfs.blockstore.wal;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

import pack.block.blockstore.BlockStoreMetaData;
import pack.block.blockstore.hdfs.blockstore.wal.WalFile.Reader;

public class WalFileFactoryPackFileSync extends WalFileFactory {

  private final static Logger LOGGER = LoggerFactory.getLogger(WalFileFactoryPackFileSync.class);
  private final long _maxIdleWriterTime;
  private final long _minTimeBetweenSyncs;
  private final double _syncRatePerSecond;

  public WalFileFactoryPackFileSync(FileSystem fileSystem, BlockStoreMetaData metaData) {
    super(fileSystem, metaData);
    long maxIdleWriterTime = metaData.getMaxIdleWriterTime();
    _maxIdleWriterTime = maxIdleWriterTime == 0 ? BlockStoreMetaData.DEFAULT_MAX_IDLE_WRITER_TIME : maxIdleWriterTime;
    long minTimeBetweenSyncs = metaData.getMinTimeBetweenSyncs();
    _minTimeBetweenSyncs = minTimeBetweenSyncs == 0 ? BlockStoreMetaData.DEFAULT_MIN_TIME_BETWEEN_SYNCS : minTimeBetweenSyncs;
    double syncRatePerSecond = metaData.getSyncRatePerSecond();
    _syncRatePerSecond = syncRatePerSecond == 0.0 ? BlockStoreMetaData.DEFAULT_SYNC_RATE_PER_SECOND : syncRatePerSecond;
  }

  public WalFile.Writer create(Path path) throws IOException {
    return new InternalWriter(_fileSystem, path, _maxIdleWriterTime, _minTimeBetweenSyncs, _syncRatePerSecond);
  }

  private static class InternalWriter extends WalFile.Writer {

    private static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";
    private final AtomicBoolean _running = new AtomicBoolean(true);
    private final AtomicReference<FSDataOutputStream> _outputStream = new AtomicReference<FSDataOutputStream>();
    private final AtomicLong _lastFlush = new AtomicLong();
    private final AtomicLong _lastFlushTime = new AtomicLong(System.nanoTime());
    private final ExecutorService _executor = Executors.newSingleThreadExecutor();
    private final FileSystem _fileSystem;
    private final Path _path;
    private final long _maxIdleWriterTime;
    private final long _minTimeBetweenSyncs;
    private final Object _flushLock = new Object();
    private final RateLimiter _rateLimiter;
    private final AtomicBoolean _error = new AtomicBoolean();

    public InternalWriter(FileSystem fileSystem, Path path, long maxIdleWriterTime, long minTimeBetweenSyncs,
        double syncRatePerSecond) throws IOException {
      _rateLimiter = RateLimiter.create(syncRatePerSecond);
      _maxIdleWriterTime = maxIdleWriterTime;
      _minTimeBetweenSyncs = minTimeBetweenSyncs;
      _fileSystem = fileSystem;
      _path = path;
      _executor.submit(() -> {
        flushLoop(path);
      });
    }

    private void flushLoop(Path path) {
      while (_running.get()) {
        try {
          flush();
        } catch (IOException e) {
          LOGGER.error("Error during flush of writer " + path, e);
        }
        long time = System.nanoTime() - _lastFlushTime.get();
        if (_outputStream.get() != null) {
          if (time >= _maxIdleWriterTime) {
            try {
              closeWriter();
            } catch (IOException e) {
              _error.set(true);
              LOGGER.error("Error during closing of idle writer " + path, e);
            }
          }
        }
        try {
          Thread.sleep(_minTimeBetweenSyncs);
        } catch (InterruptedException e) {
          if (_running.get()) {
            LOGGER.error("Unknown error", e);
          } else {
            return;
          }
        }
      }
    }

    private synchronized void closeWriter() throws IOException {
      FSDataOutputStream output = _outputStream.getAndSet(null);
      if (output != null) {
        LOGGER.info("Closing idle writer.");
        output.close();
      }
    }

    private synchronized FSDataOutputStream getOutputStream() throws IOException {
      FSDataOutputStream output = _outputStream.get();
      if (output == null) {
        if (_fileSystem.exists(_path)) {
          output = _fileSystem.append(_path);
          _lastFlushTime.set(System.nanoTime());
        } else {
          output = _fileSystem.create(_path, true, _fileSystem.getConf()
                                                              .getInt(IO_FILE_BUFFER_SIZE, 4096),
              _fileSystem.getDefaultReplication(_path), _fileSystem.getDefaultBlockSize(_path) * 2);
          _lastFlushTime.set(System.nanoTime());
        }
        _outputStream.set(output);
      }
      return output;
    }

    @Override
    public void close() throws IOException {
      _running.set(false);
      _executor.shutdownNow();
      synchronized (_flushLock) {
        try {
          FSDataOutputStream output = _outputStream.getAndSet(null);
          if (output != null) {
            output.close();
          }
        } catch (IOException e) {
          _error.set(true);
          throw e;
        }
      }
    }

    @Override
    public void append(WalKeyWritable key, BytesWritable value) throws IOException {
      synchronized (_flushLock) {
        try {
          FSDataOutputStream output = getOutputStream();
          key.write(output);
          value.write(output);
        } catch (IOException e) {
          _error.set(true);
          throw e;
        }
      }
    }

    @Override
    public long getSize() throws IOException {
      synchronized (_flushLock) {
        try {
          return getOutputStream().getPos();
        } catch (IOException e) {
          _error.set(true);
          throw e;
        }
      }
    }

    @Override
    public void flush() throws IOException {
      try {
        if (shouldFlushOutputStream()) {
          performFlush();
        }
      } catch (IOException e) {
        _error.set(true);
        throw e;
      }
    }

    private void performFlush() throws IOException {
      synchronized (_flushLock) {
        long lf = _lastFlush.get();
        long lft = _lastFlushTime.get();
        long now = System.nanoTime();

        try {
          FSDataOutputStream output = getOutputStream();
          long pos = output.getPos();
          if (lf != pos) {
            LOGGER.info("Last flush {} ago: Last flush pos {} last flush time {}", now - lft, lf, lft);
            output.hflush();
          }
          _lastFlush.set(pos);
          _lastFlushTime.set(System.nanoTime());
        } catch (IOException e) {
          if (e instanceof ClosedChannelException && !_running.get()) {
            return;
          }
          LOGGER.error("Error during flush of " + _path, e);
          throw e;
        }
      }
    }

    private boolean shouldFlushOutputStream() {
      return _rateLimiter.tryAcquire();
    }

    @Override
    public boolean isErrorState() {
      return _error.get();
    }

  }

  public WalFile.Reader open(Path path) throws IOException {
    FileStatus fileStatus = _fileSystem.getFileStatus(path);
    long len = fileStatus.getLen();
    FSDataInputStream inputStream = _fileSystem.open(path);
    return new WalFile.Reader() {

      @Override
      public void close() throws IOException {
        inputStream.close();
      }

      @Override
      public boolean next(WalKeyWritable key, BytesWritable value) throws IOException {
        if (inputStream.getPos() >= len) {
          return false;
        }
        key.readFields(inputStream);
        value.readFields(inputStream);
        return true;
      }
    };
  }

  @Override
  public void recover(Path src, Path dst) throws IOException {
    try (FSDataOutputStream outputStream = _fileSystem.create(dst, true)) {
      try (Reader reader = open(src)) {
        WalKeyWritable key = new WalKeyWritable();
        BytesWritable value = new BytesWritable();
        while (true) {
          boolean next;
          try {
            next = reader.next(key, value);
          } catch (EOFException e) {
            LOGGER.info("EOF reached for {}", src);
            return;
          }
          if (next) {
            key.write(outputStream);
            value.write(outputStream);
          } else {
            return;
          }
        }
      }
    }
  }

}
