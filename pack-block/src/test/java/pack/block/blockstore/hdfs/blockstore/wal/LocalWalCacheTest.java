package pack.block.blockstore.hdfs.blockstore.wal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.io.MD5Hash;
import org.junit.Test;

import pack.block.blockstore.hdfs.file.ReadRequest;

public class LocalWalCacheTest {

  @Test
  public void testLocalWalCacheTest() throws Throwable {
    File file = new File("./target/tmp/LocalWalCacheTest/test");
    file.getParentFile()
        .mkdirs();
    long maxLength = 1024l * 1024l * 1024l;
    AtomicLong length = new AtomicLong(maxLength);
    int blockSize = 4096;
    ExecutorService service = Executors.newFixedThreadPool(100);
    try (LocalWalCache cache = new LocalWalCache(file, length, blockSize)) {
      Random random = new Random(1);
      int upperBound = (int) (maxLength / blockSize);

      ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
      WriteLock writeLock = reentrantReadWriteLock.writeLock();
      ReadLock readLock = reentrantReadWriteLock.readLock();

      int pass = 0;
      System.out.println("Pass " + pass++);
      List<Future<Void>> list = new ArrayList<>();
      for (int i = 0; i < 100000; i++) {
        byte[] writeBuf = new byte[blockSize];
        random.nextBytes(writeBuf);
        byte[] readBuf = new byte[blockSize];
        int blockId = i;
        list.add(service.submit(() -> {
          try {
            writeLock.lock();
            cache.write(blockId, ByteBuffer.wrap(writeBuf));
          } finally {
            writeLock.unlock();
          }
          boolean readBlock;
          try {
            readLock.lock();
            readBlock = cache.readBlock(new ReadRequest(blockId, 0, ByteBuffer.wrap(readBuf)));
          } finally {
            readLock.unlock();
          }
          assertFalse(readBlock);
          assertTrue(getMessage(blockId, writeBuf, readBuf), Arrays.equals(writeBuf, readBuf));
          return null;
        }));
      }
      for (Future<Void> future : list) {
        try {
          future.get();
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
    }
  }

  private String getMessage(int blockId, byte[] writeBuf, byte[] readBuf) {
    MD5Hash writeHash = MD5Hash.digest(writeBuf);
    MD5Hash readHash = MD5Hash.digest(readBuf);
    return "Block Id " + blockId + " r:" + readHash + " w:" + writeHash;
  }
}
