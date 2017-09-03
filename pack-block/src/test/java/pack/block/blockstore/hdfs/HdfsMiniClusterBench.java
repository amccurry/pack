package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import pack.block.blockstore.hdfs.kvs.HdfsKeyValueStore;

public class HdfsMiniClusterBench {

  private static final int NUMBER_OF_WRITES = 100000;

  private static final Timer hdfsKeyValueTimer = new Timer("test", true);

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
//      long writeFile = writeFile(fileSystem, path, buf);
      // long writeFileFlush = writeFileFlush(fileSystem, path, buf);
      // long writeFileSync = writeFileSync(fileSystem, path, buf);
      // long writeFileFlushAsync = writeFileFlushAsync(fileSystem, path, buf);
      // long writeFileSyncAsync = writeFileSyncAsync(fileSystem, path, buf);
      long writeKvs = writeKvs(fileSystem, path, buf);
//      long writeKvsSync = writeKvsSync(fileSystem, path, buf);
//      long writeKvsFlush = writeKvsFlush(fileSystem, path, buf);

      System.out.println("================");
//      System.out.println("writeFile " + writeFile / 1_000_000.0 + " ms");
      // System.out.println("writeFileFlush " + writeFileFlush / 1_000_000.0 + "
      // ms");
      // System.out.println("writeFileSync " + writeFileSync / 1_000_000.0 + "
      // ms");
      // System.out.println("writeFileFlushAsync " + writeFileFlushAsync /
      // 1_000_000.0 + " ms");
      // System.out.println("writeFileSyncAsync " + writeFileSyncAsync /
      // 1_000_000.0 + " ms");
      System.out.println("writeKvs " + writeKvs / 1_000_000.0 + " ms");
//      System.out.println("writeKvsSync " + writeKvsSync / 1_000_000.0 + " ms");
//      System.out.println("writeKvsFlush " + writeKvsFlush / 1_000_000.0 + " ms");
    }
    cluster.shutdown();
  }

  private static long writeKvsFlush(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (HdfsKeyValueStore hdfsKeyValueStore = new HdfsKeyValueStore("test", false, hdfsKeyValueTimer,
        fileSystem.getConf(), path)) {
      long start = System.nanoTime();

      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        hdfsKeyValueStore.put(i, ByteBuffer.wrap(buf));
        hdfsKeyValueStore.flush();
      }
      long end = System.nanoTime();
      return end - start;
    }
  }

  private static long writeKvsSync(DistributedFileSystem fileSystem, Path path, byte[] buf) throws IOException {
    fileSystem.delete(path, true);
    fileSystem.mkdirs(path);
    try (HdfsKeyValueStore hdfsKeyValueStore = new HdfsKeyValueStore("test", false, hdfsKeyValueTimer,
        fileSystem.getConf(), path)) {
      long start = System.nanoTime();

      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
        hdfsKeyValueStore.put(i, ByteBuffer.wrap(buf));
        hdfsKeyValueStore.sync();
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
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
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
      for (int i = 0; i < NUMBER_OF_WRITES; i++) {
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
