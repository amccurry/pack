package pack.distributed.storage.monitor;

import java.io.IOException;

public class PackWriteBlockMonitorFactory {

  public WriteBlockMonitor create(String volumeName) throws IOException {
    return new PackWriteBlockMonitorEmbedded(volumeName);
  }

}
