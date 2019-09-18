package pack.iscsi.wal.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import pack.iscsi.io.FileIO;
import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.wal.BlockJournalRange;
import pack.iscsi.wal.WalTestSetup;
import pack.iscsi.wal.remote.RemoteWriteAheadLogClient.RemoteWriteAheadLogClientConfig;
import pack.iscsi.wal.remote.RemoteWriteAheadLogServer.RemoteWriteAheadLogServerConfig;

public class RemoteWriteAheadLogTest {

  private static final File DIR = new File("./target/tmp/RemoteWriteAheadLogTest");
  private RemoteWriteAheadLogServer _server;

  @Before
  public void setup() throws Exception {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
    _server = new RemoteWriteAheadLogServer(RemoteWriteAheadLogServerConfig.builder()
                                                                           .maxEntryPayload(64000)
                                                                           .walLogDir(new File(DIR, "server"))
                                                                           .port(0)
                                                                           .curatorFramework(
                                                                               WalTestSetup.getCuratorFramework())
                                                                           .build());
    _server.start(false);
  }

  @After
  public void teardown() throws Exception {
    _server.stop();
  }

  @Test
  public void testRemoteWriteAheadLog() throws Exception {
    long timeout = TimeUnit.SECONDS.toMillis(3);
    RemoteWriteAheadLogClientConfig config = RemoteWriteAheadLogClientConfig.builder()
                                                                            .curatorFramework(
                                                                                WalTestSetup.getCuratorFramework())
                                                                            .timeout(timeout)
                                                                            .build();
    try (RemoteWriteAheadLogClient client = new RemoteWriteAheadLogClient(config)) {

      File dir = new File(DIR, "client");
      dir.mkdirs();

      long volumeId = 0;
      long blockId = 0;
      long generation = 0;
      long position = 1000;
      int length = 10_000_000;
      byte[] bytes = new byte[8192];
      Random random = new Random();
      File expected = new File(dir, UUID.randomUUID()
                                        .toString());
      try (RandomAccessFile raf = new RandomAccessFile(expected, "rw")) {
        raf.setLength(length);
        for (int i = 0; i < 100; i++) {
          generation++;
          position = random.nextInt(length - bytes.length);
          random.nextBytes(bytes);
          client.write(volumeId, blockId, generation, position, bytes);
          if (i % 35 == 0) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(4));
          }
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
          onDiskGeneration = client.recoverFromJournal(randomAccessIO, journalRange, onDiskGeneration);
        }
      }
      compareFile(expected, actual);

      client.releaseJournals(volumeId, blockId, onDiskGeneration);
    }
  }

  private void compareFile(File expectedFile, File actualFile) throws IOException {
    try (RandomAccessFile expected = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile actual = new RandomAccessFile(actualFile, "r")) {
        assertEquals(expectedFile.length(), actualFile.length());
        long length = expectedFile.length();
        byte[] buffer1 = new byte[1024];
        byte[] buffer2 = new byte[1024];
        while (length > 0) {
          int len = (int) Math.min(length, buffer1.length);
          expected.readFully(buffer1, 0, len);
          actual.readFully(buffer2, 0, len);
          assertTrue(Arrays.equals(buffer1, buffer2));
          length -= len;
        }
      }
    }
  }

}
