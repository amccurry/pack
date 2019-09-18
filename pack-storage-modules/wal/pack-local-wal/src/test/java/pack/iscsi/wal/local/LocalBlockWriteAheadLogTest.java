package pack.iscsi.wal.local;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import pack.iscsi.io.IOUtils;
import pack.iscsi.spi.wal.BlockJournalResult;
import pack.iscsi.wal.local.LocalBlockWriteAheadLog.LocalBlockWriteAheadLogConfig;

public class LocalBlockWriteAheadLogTest {

  private static final File DIR = new File("./target/tmp/LocalBlockWriteAheadLogTest");

  @Before
  public void setup() {
    IOUtils.rmr(DIR);
    DIR.mkdirs();
  }

  @Test
  public void testLocalBlockWriteAheadLog() throws IOException {
    LocalBlockWriteAheadLogConfig config = LocalBlockWriteAheadLogConfig.builder()
                                                                        .walLogDir(DIR)
                                                                        .build();
    try (LocalBlockWriteAheadLog log = new LocalBlockWriteAheadLog(config)) {
      BlockJournalResult result = log.write(0, 0, 1, 0, new byte[1000]);
      result.get();
      log.releaseJournals(0, 0, 1);
    }
  }

}
