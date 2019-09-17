package pack.iscsi.spi.wal;

import java.io.IOException;

public interface BlockWriteAheadLogResult {
  
  void get() throws IOException;
  
}
