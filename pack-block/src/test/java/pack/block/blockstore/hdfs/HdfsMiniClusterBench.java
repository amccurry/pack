package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import pack.block.blockstore.hdfs.kvs.HdfsKeyValueStore;

public class HdfsMiniClusterBench {

  private static final int NUMBER_OF_WRITES = 10000;

  private static final Timer hdfsKeyValueTimer = new Timer("test", true);

  private static long _5_SECONDS = TimeUnit.SECONDS.toNanos(5);

  public static void main(String[] args) throws IOException, InterruptedException {
    File storePathDir = new File("./test");
    Configuration configuration = new Configuration();
    String storePath = storePathDir.getAbsolutePath();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, storePath);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(configuration).build();
    DistributedFileSystem fileSystem = cluster.getFileSystem();

    int size = 4 * 1024;
    byte[] buf = new byte[size];
    Random random = new Random();
    random.nextBytes(buf);
    Path path = new Path("/test");
    for (int i = 0; i < 10000; i++) {
      // System.out.println("writeFile");
      // long writeFile = writeFile(fileSystem, path, buf);
      // System.out.println("writeFileFlush");
      // long writeFileFlush = writeFileFlush(fileSystem, path, buf);
      // System.out.println("writeFileSync");
      // long writeFileSync = writeFileSync(fileSystem, path, buf);
      System.out.println("writeFileFlushAsync");
      long writeFileFlushAsync = writeFileFlushAsync(fileSystem, path, buf);
      // System.out.println("writeFileSyncAsync");
      // long writeFileSyncAsync = writeFileSyncAsync(fileSystem, path, buf);
      System.out.println("writeKvs");
      long writeKvs = writeKvs(fileSystem, path, buf);
      System.out.println("writeKvsSync1");
      long writeKvsSync1 = writeKvsSync(true, fileSystem, path, buf);
      System.out.println("writeKvsSync2");
      long writeKvsSync2 = writeKvsSync(false, fileSystem, path, buf);
      System.out.println("writeKvsFlush1");
      long writeKvsFlush1 = writeKvsFlush(true, fileSystem, path, buf);
      System.out.println("writeKvsFlush2");
      long writeKvsFlush2 = writeKvsFlush(false, fileSystem, path, buf);

      System.out.println("================");
      // System.out.println("writeFile " + writeFile / 1_000_000.0 + " ms");
      // System.out.println("writeFileFlush " + writeFileFlush / 1_000_000.0 + "
      // ms");
      // System.out.println("writeFileSync " + writeFileSync / 1_000_000.0 + "
      // ms");
      System.out.println("writeFileFlushAsync " + writeFileFlushAsync / 1_000_000.0 + " ms");
      // System.out.println("writeFileSyncAsync " + writeFileSyncAsync /
      // 1_000_000.0 + " ms");
      System.out.println("writeKvs " + writeKvs / 1_000_000.0 + " ms");
      System.out.println("writeKvsSync1 " + writeKvsSync1 / 1_000_000.0 + " ms");
      System.out.println("writeKvsFlush2 " + writeKvsFlush1 / 1_000_000.0 + " ms");
      System.out.println("writeKvsSync1 " + writeKvsSync2 / 1_000_000.0 + " ms");
      System.out.println("writeKvsFlush2 " + writeKvsFlush2 / 1_000_000.0 + " ms");
    }
    cluster.shutdown();
  }

  private static long writeKvsFlush(boolean sync, DistributedFileSystem fileSystem, Path path, byte[] buf)
      throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (HdfsKeyValueStore hdfsKeyValueStore = new HdfsKeyValueStore("test", false, hdfsKeyValueTimer,
        fileSystem.getConf(), path)) {
      long start = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        hdfsKeyValueStore.put(i, ByteBuffer.wrap(buf));
        hdfsKeyValueStore.flush(sync);
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeKvsSync(boolean sync, DistributedFileSystem fileSystem, Path path, byte[] buf)
      throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (HdfsKeyValueStore hdfsKeyValueStore = new HdfsKeyValueStore("test", false, hdfsKeyValueTimer,
        fileSystem.getConf(), path)) {
      long start = System.nanoTime();
      long lastReport = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        if (lastReport + _5_SECONDS < System.nanoTime()) {
          System.out.println("block " + i);
          lastReport = System.nanoTime();
        }
        hdfsKeyValueStore.put(i, ByteBuffer.wrap(buf));
        hdfsKeyValueStore.sync(sync);
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeFileSyncAsync(DistributedFileSystem fileSystem, Path path, byte[] buf)
      throws IOException, InterruptedException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (FSDataOutputStream outputStream = fileSystem.create(new Path(path, UUID.randomUUID()
                                                                                .toString()))) {
      AtomicBoolean running = new AtomicBoolean(true);
      Thread thread = new Thread(() -> {
        while (running.get()) {
          try {
            outputStream.hsync();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e2) {
            e2.printStackTrace();
          }
        }
      });
      thread.start();
      long start = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        outputStream.write(buf);
      }
      running.set(false);
      thread.join();
      outputStream.hsync();
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeFileFlushAsync(DistributedFileSystem fileSystem, Path path, byte[] buf)
      throws IOException, InterruptedException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (FSDataOutputStream outputStream = fileSystem.create(new Path(path, UUID.randomUUID()
                                                                                .toString()))) {
      AtomicBoolean running = new AtomicBoolean(true);
      Thread thread = new Thread(() -> {
        while (running.get()) {
          try {
            outputStream.hflush();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e2) {
            e2.printStackTrace();
          }
        }
      });
      thread.start();
      long start = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        outputStream.write(buf);
      }
      running.set(false);
      thread.join();
      outputStream.hflush();
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeKvs(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (HdfsKeyValueStore hdfsKeyValueStore = new HdfsKeyValueStore("test", false, hdfsKeyValueTimer,
        fileSystem.getConf(), path)) {
      long start = System.nanoTime();
      long lastReport = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        if (lastReport + _5_SECONDS < System.nanoTime()) {
          System.out.println("block " + i);
          lastReport = System.nanoTime();
        }
        hdfsKeyValueStore.put(i, ByteBuffer.wrap(buf));
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeFileFlush(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (FSDataOutputStream outputStream = fileSystem.create(new Path(path, UUID.randomUUID()
                                                                                .toString()))) {
      long start = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        outputStream.write(buf);
        outputStream.hflush();
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeFileSync(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (FSDataOutputStream outputStream = fileSystem.create(new Path(path, UUID.randomUUID()
                                                                                .toString()))) {
      long start = System.nanoTime();
      long lastReport = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        if (lastReport + _5_SECONDS < System.nanoTime()) {
          System.out.println("block " + i);
          lastReport = System.nanoTime();
        }
        outputStream.write(buf);
        outputStream.hsync();
      }
      long end = System.nanoTime();
      return end - start;
    }

  }

  private static long writeFile(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (FSDataOutputStream outputStream = fileSystem.create(new Path(path, UUID.randomUUID()
                                                                                .toString()))) {
      long start = System.nanoTime();
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        outputStream.write(buf);
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

}
