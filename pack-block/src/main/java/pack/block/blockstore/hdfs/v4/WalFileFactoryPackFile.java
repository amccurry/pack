package pack.block.blockstore.hdfs.v4;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.hdfs.HdfsMetaData;
import pack.block.blockstore.hdfs.file.WalKeyWritable;
import pack.block.blockstore.hdfs.v4.WalFile.Reader;

public class WalFileFactoryPackFile extends WalFileFactory {

  private final static Logger LOGGER = LoggerFactory.getLogger(WalFileFactoryPackFile.class);

  public WalFileFactoryPackFile(FileSystem fileSystem, HdfsMetaData metaData) {
    super(fileSystem, metaData);
  }

  public WalFile.Writer create(Path path) throws IOException {
    FSDataOutputStream outputStream = _fileSystem.create(path, true);

    AtomicLong flushPoint = new AtomicLong();
    AtomicBoolean running = new AtomicBoolean(true);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new Runnable() {
      private long lastFlush = 0;

      @Override
      public void run() {
        while (running.get()) {
          if (lastFlush < flushPoint.get()) {
            try {
              long pos = outputStream.getPos();
              outputStream.hflush();
              lastFlush = pos;
            } catch (IOException e) {
              LOGGER.error("Error during flush of " + path, e);
            }
          } else {
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
      }
    });

    return new WalFile.Writer() {

      @Override
      public void close() throws IOException {
        running.set(false);
        outputStream.close();
        executor.shutdownNow();
      }

      @Override
      public void append(WalKeyWritable key, BytesWritable value) throws IOException {
        key.write(outputStream);
        value.write(outputStream);
      }

      @Override
      public long getLength() throws IOException {
        return outputStream.getPos();
      }

      @Override
      public void flush() throws IOException {
        long pos = outputStream.getPos();
        flushPoint.set(pos);
      }

    };
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
