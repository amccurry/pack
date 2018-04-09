package pack.distributed.storage.status;

import java.io.Closeable;

import pack.distributed.storage.monitor.WriteBlockMonitor;

public interface BroadcastServerManager extends Closeable {

  boolean isLeader(String name);

  void register(String name, WriteBlockMonitor monitor);

  void broadcastToAllServers(BlockUpdateInfoBatch updateBlockIdBatch) throws InterruptedException;

}