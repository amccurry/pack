package utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.storage.ReadRequest;

public class IOUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(IOUtils.class);

  public static void closeQuietly(Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable : closeables) {
      if (closeable != null) {
        try {
          closeable.close();
        } catch (IOException e) {

        }
      }
    }
  }

  public static void checkFutureIsRunning(Future<Void> future) {
    if (future.isDone()) {
      try {
        future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException("Future " + future + " failed");
      } catch (ExecutionException e) {
        Throwable throwable = e.getCause();
        throw (RuntimeException) throwable;
      }
    }
  }

  public static List<ReadRequest> createRequests(ByteBuffer byteBuffer, long storageIndex, int blockSize) {
    int remaining = byteBuffer.remaining();
    int bufferPosition = 0;
    List<ReadRequest> result = new ArrayList<>();
    while (remaining > 0) {
      int blockOffset = getBlockOffset(storageIndex, blockSize);
      long blockId = getBlockId(storageIndex, blockSize);
      int len = Math.min(blockSize - blockOffset, remaining);

      byteBuffer.position(bufferPosition);
      byteBuffer.limit(bufferPosition + len);

      ByteBuffer slice = byteBuffer.slice();
      result.add(new ReadRequest(blockId, blockOffset, slice));

      storageIndex += len;
      bufferPosition += len;
      remaining -= len;
    }
    return result;
  }

  public static int getBlockOffset(long position, int blockSize) {
    return (int) (position % blockSize);
  }

  public static long getBlockId(long position, int blockSize) {
    return position / blockSize;
  }

  public static void assertIsValidForWriting(long storageIndex, int length, int blockSize) throws IOException {
    int blockOffset = getBlockOffset(storageIndex, blockSize);
    if (blockOffset != 0) {
      LOGGER.error("storage index {} is invalid produced blockOffset of {} with blockSize set to {}", storageIndex,
          blockOffset, blockSize);
      throw new IOException("storage index " + storageIndex + " is invalid produced blockOffset of " + blockOffset
          + " with blockSize set to " + blockSize);
    }
    if (length % blockSize != 0) {
      LOGGER.error("block length {} is invalid with blockSize set to {}", length, blockSize);
      throw new IOException("block length " + length + " is invalid with blockSize set to " + blockSize);
    }
  }

  public static void rmr(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rmr(f);
      }
    }
    file.delete();
  }

}
