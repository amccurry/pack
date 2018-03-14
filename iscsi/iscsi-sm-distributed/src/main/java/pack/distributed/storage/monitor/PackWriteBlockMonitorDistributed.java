package pack.distributed.storage.monitor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.monitor.rpc.MultiWriteBlockMonitor;

public class PackWriteBlockMonitorDistributed implements WriteBlockMonitor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackWriteBlockMonitorDistributed.class);

  private final MultiWriteBlockMonitor _client;
  private final int _volumeId;
  private final int _serverId;

  public PackWriteBlockMonitorDistributed(String volumeName, MultiWriteBlockMonitor client, int serverId)
      throws IOException {
    _serverId = serverId;
    _client = client;
    _volumeId = client.registerVolume(volumeName);
  }

  public void resetDirtyBlock(int blockId, long transId) {
    try {
      _client.resetDirtyBlock(_serverId, _volumeId, blockId, transId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void addDirtyBlock(int blockId, long transId) {
    try {
      _client.addDirtyBlock(_volumeId, blockId, transId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void waitIfNeededForSync(int blockId) {
    try {
      _client.waitIfNeededForSync(_serverId, _volumeId, blockId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
