package pack.iscsi.spi.wal;

import java.io.IOException;

public interface BlockJournalResult {
  
  void get() throws IOException;
  
}
