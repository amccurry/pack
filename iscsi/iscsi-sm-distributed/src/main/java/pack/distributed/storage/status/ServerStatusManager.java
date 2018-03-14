package pack.distributed.storage.status;

import pack.distributed.storage.monitor.WriteBlockMonitor;

public interface ServerStatusManager {

  boolean isLeader(String name);

  void register(String name, WriteBlockMonitor monitor);

  void broadcastToAllServers(String name, int blockId, long transId);

}