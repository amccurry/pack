package pack.iscsi.error;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.utils.PackUtils;

public class NoExceptions {

  private static final Logger LOGGER = LoggerFactory.getLogger(NoExceptions.class);

  public static IStorageModule retryForever(IStorageModule module) {
    return new NoExceptionsIStorageModule(module);
  }

  static class NoExceptionsIStorageModule implements IStorageModule {

    private final IStorageModule _module;

    public NoExceptionsIStorageModule(IStorageModule module) {
      _module = module;
    }

    @Override
    public int checkBounds(long logicalBlockAddress, int transferLengthInBlocks) {
      while (!Thread.interrupted()) {
        try {
          return _module.checkBounds(logicalBlockAddress, transferLengthInBlocks);
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
      return 0;
    }

    @Override
    public long getSizeInBlocks() {
      while (!Thread.interrupted()) {
        try {
          return _module.getSizeInBlocks();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
      return 0;
    }

    @Override
    public void read(byte[] bytes, long storageIndex) throws IOException {
      while (!Thread.interrupted()) {
        try {
          _module.read(bytes, storageIndex);
          return;
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
    }

    @Override
    public void read(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
        Integer commandSequenceNumber) throws IOException {
      while (!Thread.interrupted()) {
        try {
          _module.read(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber);
          return;
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
    }

    @Override
    public void write(byte[] bytes, long storageIndex) throws IOException {
      while (!Thread.interrupted()) {
        try {
          _module.write(bytes, storageIndex);
          return;
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
    }

    @Override
    public void write(byte[] bytes, long storageIndex, InetAddress address, int port, int initiatorTaskTag,
        Integer commandSequenceNumber, Integer dataSequenceNumber, Integer targetTransferTag) throws IOException {
      while (!Thread.interrupted()) {
        try {
          _module.write(bytes, storageIndex, address, port, initiatorTaskTag, commandSequenceNumber, dataSequenceNumber,
              targetTransferTag);
          return;
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
    }

    @Override
    public void close() throws IOException {
      PackUtils.close(LOGGER, _module);
    }

    @Override
    public void flushWrites() throws IOException {
      while (!Thread.interrupted()) {
        try {
          _module.flushWrites();
          return;
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
    }

    @Override
    public int getBlockSize() {
      while (!Thread.interrupted()) {
        try {
          return _module.getBlockSize();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
      return 0;
    }

    @Override
    public UUID getSerialId() {
      while (!Thread.interrupted()) {
        try {
          return _module.getSerialId();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
          sleep();
        }
      }
      return null;
    }

  }

  public static void sleep() {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException e) {

    }
  }
}
