package pack.iscsi.bk.wal;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

import pack.iscsi.bk.BKTestSetup;
import pack.iscsi.io.FileIO;
import pack.iscsi.spi.RandomAccessIO;
import pack.iscsi.spi.wal.BlockWriteAheadLogResult;

public class BookKeeperWriteAheadLogTest {

  @Test
  public void testBookKeeperWriteAheadLog() throws Exception {
    BookKeeper bookKeeper = BKTestSetup.getBookKeeper();
    CuratorFramework curatorFramework = BKTestSetup.getCuratorFramework();
    BookKeeperWriteAheadLogConfig config = BookKeeperWriteAheadLogConfig.builder()
                                                                        .ackQuorumSize(1)
                                                                        .writeQuorumSize(1)
                                                                        .ensSize(1)
                                                                        .bookKeeper(bookKeeper)
                                                                        .curatorFramework(curatorFramework)
                                                                        .build();
    long volumeId = 1;
    long blockId = 1;
    long position = 12345;
    long generation = 1;
    try (BookKeeperWriteAheadLog wal = new BookKeeperWriteAheadLog(config)) {
      byte[] bytes = new byte[] { 1 };
      BlockWriteAheadLogResult result = wal.write(volumeId, blockId, generation, position, bytes, 0, bytes.length);
      result.get();
    }

    try (BookKeeperWriteAheadLog wal = new BookKeeperWriteAheadLog(config)) {
      File file = File.createTempFile("bk.", ".test");

      try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, 4096, "rw")) {
        randomAccessIO.setLength(100_000);
        wal.recover(randomAccessIO, volumeId, blockId, 0);
      }
      try (RandomAccessIO randomAccessIO = FileIO.openRandomAccess(file, 4096, "rw")) {
        randomAccessIO.setLength(file.length());
        byte[] buffer = new byte[1];
        randomAccessIO.readFully(position, buffer);
        assertEquals(1, buffer[1] & 0xff);
        wal.release(volumeId, blockId, generation);
      }
    }
  }

}
