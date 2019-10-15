package pack.iscsi.wal.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.async.AsyncCompletableFuture;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.spi.wal.BlockRecoveryWriter;
import pack.iscsi.wal.WalTestSetup;
import pack.iscsi.wal.remote.RemoteWALClient.RemoteWALClientConfig;
import pack.iscsi.wal.remote.RemoteWALServer.RemoteWriteAheadLogServerConfig;

public class RemoteWriteAheadLogTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteWriteAheadLogTest.class);
  private static final File DIR = new File("./target/tmp/RemoteWriteAheadLogTest");
  private RemoteWALServer _server;

  @Before
  public void setup() throws Exception {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
    _server = new RemoteWALServer(RemoteWriteAheadLogServerConfig.builder()
                                                                 .maxEntryPayload(64000)
                                                                 .walLogDir(new File(DIR, "server"))
                                                                 .port(0)
                                                                 .curatorFramework(WalTestSetup.getCuratorFramework())
                                                                 .build());
    _server.start(false);
  }

  @After
  public void teardown() throws Exception {
    if (_server != null) {
      _server.stop();
    }
  }

  @Test
  public void testRemoteWriteAheadLog() throws Exception {
    long timeout = TimeUnit.SECONDS.toMillis(3);
    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                        .curatorFramework(WalTestSetup.getCuratorFramework())
                                                        .timeout(timeout)
                                                        .build();
    try (RemoteWALClient client = new RemoteWALClient(config)) {

      long volumeId = 0;
      long blockId = 0;

      File dir = new File(DIR, "client");
      dir.mkdirs();
      runTest(client, volumeId, blockId, dir);
    }
  }

  @Test
  public void testRemoteWriteAheadLogThreaded() throws Exception {
    long timeout = TimeUnit.SECONDS.toMillis(3);
    RemoteWALClientConfig config = RemoteWALClientConfig.builder()
                                                        .curatorFramework(WalTestSetup.getCuratorFramework())
                                                        .timeout(timeout)
                                                        .build();
    int threads = 10;
    ExecutorService pool = Executors.newCachedThreadPool();
    try (RemoteWALClient client = new RemoteWALClient(config)) {
      long volumeId = 0;
      File dir = new File(DIR, "client");
      dir.mkdirs();
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        long blockId = i;
        futures.add(pool.submit(() -> {
          runTest(client, volumeId, blockId, dir);
          return null;
        }));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
    }
    pool.shutdownNow();
  }

  private void runTest(RemoteWALClient client, long volumeId, long blockId, File dir)
      throws IOException, InterruptedException, FileNotFoundException {

//    FileIO.setDirectIOEnabled(false);
    long generation = 0;
    long position = 1000;
    int length = 10_000_000;
    byte[] bytes = new byte[8192];
    long seed = getSeed();
    seed = -6045049710333444935L;
    Random random = new Random(seed);
    File expected = new File(dir, UUID.randomUUID()
                                      .toString());

    int passes = 100;

    try (RandomAccessFile raf = new RandomAccessFile(expected, "rw")) {
      raf.setLength(length);
      for (int i = 0; i < passes; i++) {
        LOGGER.info("Running pass {}", i);
        generation++;
        position = random.nextInt(length - bytes.length);
        random.nextBytes(bytes);
        LOGGER.info("Client write - pass {}", i);
        AsyncCompletableFuture future = client.write(volumeId, blockId, generation, position, bytes);
        future.get();
        if (i % 35 == 0) {
          LOGGER.info("Sleeping - pass {}", i);
          Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        }
        LOGGER.info("Writing locally - pass {}", i);
        raf.seek(position);
        raf.write(bytes);

      }
    }

    long onDiskGeneration = 0;

    List<BlockJournalRange> journalRanges = client.getJournalRanges(volumeId, blockId, onDiskGeneration, true);
    Collections.sort(journalRanges);

    File actual = new File(dir, UUID.randomUUID()
                                    .toString());
    try (RandomAccessFile raf = new RandomAccessFile(actual, "rw")) {
      raf.setLength(length);
    }
    try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(actual, 4096, "rw")) {
      for (BlockJournalRange journalRange : journalRanges) {
        onDiskGeneration = client.recoverFromJournal(BlockRecoveryWriter.toBlockRecoveryWriter(0, randomAccessIO),
            journalRange, onDiskGeneration);
      }
    }
    compareFile(seed, expected, actual);

    client.releaseJournals(volumeId, blockId, onDiskGeneration);
  }

  private long getSeed() {
    Random random = new Random();
    return random.nextLong();
  }

  private void compareFile(long seed, File expectedFile, File actualFile) throws IOException {
    try (RandomAccessFile expected = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile actual = new RandomAccessFile(actualFile, "r")) {
        assertEquals("seed=" + seed, expectedFile.length(), actualFile.length());
        long length = expectedFile.length();
        byte[] buffer1 = new byte[1024];
        byte[] buffer2 = new byte[1024];
        while (length > 0) {
          int len = (int) Math.min(length, buffer1.length);
          expected.readFully(buffer1, 0, len);
          actual.readFully(buffer2, 0, len);
          try {
            for (int i = 0; i < len; i++) {
              assertEquals("seed=" + seed + " epos=" + expected.getFilePointer() + " apos " + actual.getFilePointer()
                  + " i=" + i, buffer1[i], buffer2[i]);
            }
          } catch (AssertionError e) {
            System.out.println();
            throw e;
          }
          length -= len;
        }
      }
    }
  }

}
