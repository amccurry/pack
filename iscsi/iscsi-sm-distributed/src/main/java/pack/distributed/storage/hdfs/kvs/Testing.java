package pack.distributed.storage.hdfs.kvs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import pack.distributed.storage.PackConfig;

public class Testing {

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration configuration = PackConfig.getConfiguration();
    UserGroupInformation.setConfiguration(configuration);

    int c = 0;
    Timer hdfsKeyValueTimer = new Timer(true);
    Path path = new Path(args[c++]);
    Random random = new Random();
    byte[] buf1 = new byte[256 * 1024];
    random.nextBytes(buf1);
    byte[] buf2 = new byte[8192];
    random.nextBytes(buf2);
    byte[] buf3 = new byte[4096];
    random.nextBytes(buf3);

    int threadCount = Integer.parseInt(args[c++]);

    long maxAmountAllowedPerFile = Long.parseLong(args[c++]);

    long delay = Long.parseLong(args[c++]);

    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.delete(path, true);

    AtomicLong totalDataAtomic = new AtomicLong();

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        long start = System.nanoTime();
        while (true) {
          long totalData = totalDataAtomic.getAndSet(0);
          long now = System.nanoTime();
          double seconds = (now - start) / 1_000_000_000.0;
          double rate = totalData / seconds;
          System.out.printf("Total %,d Rate %,f%n", totalData, rate);
          totalData = 0;
          start = System.nanoTime();
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    thread.setDaemon(true);
    thread.setName("report");
    thread.start();

    try (HdfsKeyValueStore store = new HdfsKeyValueStore(false, hdfsKeyValueTimer, configuration, path,
        maxAmountAllowedPerFile)) {
      List<Thread> threads = new ArrayList<>();
      long offset = 0;

      for (int i = 0; i < threadCount; i++) {
        long o = offset;
        Thread t = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              runThroughputTest(buf1, buf2, buf3, store, totalDataAtomic, o, delay);
            } catch (Exception e) {
              e.printStackTrace();
              return;
            }
          }
        });
        t.setDaemon(true);
        t.start();
        threads.add(t);
        offset += 1_000_000_000;
      }

      for (Thread t : threads) {
        t.join();
      }
    }
  }

  private static void runThroughputTest(byte[] buf1, byte[] buf2, byte[] buf3, HdfsKeyValueStore store,
      AtomicLong totalDataAtomic, long offset, long delay) throws IOException, InterruptedException {
    BytesRef startKey = new BytesRef(offset);
    while (true) {
      {
        TransId transId = store.put(new BytesRef(offset++), new BytesRef(buf1));
        Thread.sleep(delay);
        store.sync(transId);
        totalDataAtomic.addAndGet(buf1.length);
      }
      {
        TransId transId = store.put(new BytesRef(offset++), new BytesRef(buf2));
        Thread.sleep(delay);
        store.sync(transId);
        totalDataAtomic.addAndGet(buf2.length);
      }
      {
        TransId transId = store.put(new BytesRef(offset++), new BytesRef(buf3));
        Thread.sleep(delay);
        store.sync(transId);
        totalDataAtomic.addAndGet(buf3.length);
      }
      store.deleteRange(startKey, new BytesRef(offset));
    }
  }
}
