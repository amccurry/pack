package pack.block.blockstore.hdfs.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.blockstore.BlockStore;
import pack.block.blockstore.BlockStoreMetaData;

public class RetryBlockStore implements BlockStore {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryBlockStore.class);

  private static final long MAX_WAIT_TIME = 10;

  private final BlockStore _base;

  public static RetryBlockStore wrap(BlockStore blockStore) {
    return new RetryBlockStore(blockStore);
  }

  public RetryBlockStore(BlockStore base) {
    _base = base;
  }

  private interface Exec<T> {
    T exec() throws Throwable;
  }

  private interface ExecVoid {
    void exec() throws Throwable;
  }

  @Override
  public BlockStoreMetaData getMetaData() throws IOException {
    return exec(() -> _base.getMetaData());
  }

  @Override
  public String getName() throws IOException {
    return exec(() -> _base.getName());
  }

  @Override
  public long getLength() throws IOException {
    return exec(() -> _base.getLength());
  }

  @Override
  public long lastModified() throws IOException {
    return exec(() -> _base.lastModified());
  }

  @Override
  public int write(long position, byte[] buffer, int offset, int len) throws IOException {
    return exec(() -> _base.write(position, buffer, offset, len));
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int len) throws IOException {
    return exec(() -> _base.read(position, buffer, offset, len));
  }

  @Override
  public void fsync() throws IOException {
    exec(() -> _base.fsync());
  }

  @Override
  public void close() throws IOException {
    exec(() -> _base.close());
  }

  @Override
  public void delete(long position, long length) throws IOException {
    exec(() -> _base.delete(position, length));
  }

  private void exec(ExecVoid exec) {
    exec(() -> {
      exec.exec();
      return null;
    });
  }

  private <T> T exec(Exec<T> exec) {
    long attempt = 1;
    while (true) {
      try {
        return exec.exec();
      } catch (Throwable e) {
        LOGGER.error("Unknown error, retrying attempt " + attempt, e);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(Math.min(attempt, MAX_WAIT_TIME)));
        } catch (InterruptedException ex) {
          LOGGER.error("Unknown error", ex);
        }
      }
    }
  }

}
