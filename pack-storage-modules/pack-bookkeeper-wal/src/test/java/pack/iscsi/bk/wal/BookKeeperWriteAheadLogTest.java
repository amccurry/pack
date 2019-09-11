package pack.iscsi.bk.wal;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

import pack.iscsi.bk.BKTestSetup;
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
      try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
        raf.setLength(100_000);
        try (FileChannel channel = raf.getChannel()) {
          wal.recover(channel, volumeId, blockId, 0);
        }
      }
      try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
        raf.seek(position);
        assertEquals(1, raf.read());
      }
      wal.release(volumeId, blockId, generation);
    }

  }

}
