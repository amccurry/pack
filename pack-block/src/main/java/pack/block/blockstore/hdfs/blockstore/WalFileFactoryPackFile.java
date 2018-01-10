package pack.block.blockstore.hdfs.blockstore;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.blockstore.WalFile.Reader;
import pack.block.blockstore.hdfs.file.WalKeyWritable;

public class WalFileFactoryPackFile extends WalFileFactory {

  private final static Logger LOGGER = LoggerFactory.getLogger(WalFileFactoryPackFile.class);
  private final long _maxIdleWriterTime;

  public WalFileFactoryPackFile(FileSystem fileSystem, HdfsMetaData metaData) {
    super(fileSystem, metaData);
    long maxIdleWriterTime = metaData.getMaxIdleWriterTime();
    _maxIdleWriterTime = maxIdleWriterTime == 0 ? HdfsMetaData.DEFAULT_MAX_IDLE_WRITER_TIME : maxIdleWriterTime;
  }

  public WalFile.Writer create(Path path) throws IOException {
    return new InternalWriter(_fileSystem, path, _maxIdleWriterTime);
  }

  private static class InternalWriter extends WalFile.Writer {

    private final AtomicLong flushPoint = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<FSDataOutputStream> outputStream = new AtomicReference<FSDataOutputStream>();
    private final AtomicLong lastFlush = new AtomicLong();
    private final AtomicLong lastFlushTime = new AtomicLong(System.nanoTime());
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final FileSystem _fileSystem;
    private final Path _path;
    private final long _maxIdleWriterTime;

    public InternalWriter(FileSystem fileSystem, Path path, long maxIdleWriterTime) throws IOException {
      _maxIdleWriterTime = maxIdleWriterTime;
      _fileSystem = fileSystem;
      _path = path;
      executor.submit(() -> {
        flushLoop(path);
      });
    }

    private void flushLoop(Path path) {
      while (running.get()) {
        if (lastFlush.get() < flushPoint.get()) {
          try {
            FSDataOutputStream output = getOutputStream();
            long pos = output.getPos();
            output.hflush();
            lastFlush.set(pos);
            lastFlushTime.set(System.nanoTime());
          } catch (IOException e) {
            if (e instanceof ClosedChannelException && !running.get()) {
              return;
            }
            LOGGER.error("Error during flush of " + path, e);
          }
        }

        long time = System.nanoTime() - lastFlushTime.get();
        if (outputStream.get() != null) {
          if (time >= _maxIdleWriterTime) {
            try {
              closeWriter();
            } catch (IOException e) {
              LOGGER.error("Error during closing of idle writer " + path, e);
            }
          }
        }
        try {
          Thread.sleep(TimeUnit.MILLISECONDS.toMillis(10));
        } catch (InterruptedException e) {
          if (running.get()) {
            LOGGER.error("Unknown error", e);
          } else {
            return;
          }
        }
      }
    }

    private synchronized void closeWriter() throws IOException {
      FSDataOutputStream output = outputStream.getAndSet(null);
      if (output != null) {
        LOGGER.info("Closing idle writer.");
        output.close();
      }
    }

    private synchronized FSDataOutputStream getOutputStream() throws IOException {
      FSDataOutputStream output = outputStream.get();
      if (output == null) {
        if (_fileSystem.exists(_path)) {
          output = _fileSystem.append(_path);
          lastFlushTime.set(System.nanoTime());
        } else {
          output = _fileSystem.create(_path, true);
          lastFlushTime.set(System.nanoTime());
        }
        outputStream.set(output);
      }
      return output;
    }

    @Override
    public void close() throws IOException {
      running.set(false);
      FSDataOutputStream output = outputStream.getAndSet(null);
      if (output != null) {
        output.close();
      }
      executor.shutdownNow();
    }

    @Override
    public void append(WalKeyWritable key, BytesWritable value) throws IOException {
      FSDataOutputStream output = getOutputStream();
      key.write(output);
      value.write(output);
    }

    @Override
    public long getLength() throws IOException {
      return getOutputStream().getPos();
    }

    @Override
    public void flush() throws IOException {
      long pos = getOutputStream().getPos();
      flushPoint.set(pos);
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
        try {
          while (reader.next(key, value)) {
            key.write(outputStream);
            value.write(outputStream);
          }
        } catch (EOFException e) {
          LOGGER.info("EOF reached for {}", src);
          return;
        }
      }
    }
  }

}
