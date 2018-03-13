package pack.distributed.storage.monitor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.monitor.rpc.WriteBlockMonitorRaw;

public class PackWriteBlockMonitorDistributed implements WriteBlockMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackWriteBlockMonitorDistributed.class);

  private final WriteBlockMonitorRaw _client;
  private final int _id;

  public PackWriteBlockMonitorDistributed(String name, WriteBlockMonitorRaw client) throws IOException {
    _id = client.register(name);
    _client = client;
  }

  public void resetDirtyBlock(int blockId, long transId) {
    try {
      _client.resetDirtyBlock(_id, blockId, transId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addDirtyBlock(int blockId, long transId) {
    try {
      _client.addDirtyBlock(_id, blockId, transId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void waitIfNeededForSync(int blockId) {
    try {
      _client.waitIfNeededForSync(_id, blockId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
