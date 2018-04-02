package pack.distributed.storage.broadcast;

import java.io.IOException;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.ServerStatusManager;
import pack.distributed.storage.wal.WalCacheManager;

public abstract class PackBroadcastFactory {

  public abstract PackBroadcastWriter createPackBroadcastWriter(String name, PackMetaData metaData,
      WriteBlockMonitor writeBlockMonitor, ServerStatusManager serverStatusManager) throws IOException;

  public abstract PackBroadcastReader createPackBroadcastReader(String name, PackMetaData metaData,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer) throws IOException;

}
