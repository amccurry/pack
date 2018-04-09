package pack.distributed.storage.wal;

import java.io.IOException;

import pack.distributed.storage.PackMetaData;
import pack.distributed.storage.hdfs.MaxBlockLayer;
import pack.distributed.storage.monitor.WriteBlockMonitor;
import pack.distributed.storage.status.BroadcastServerManager;
import pack.distributed.storage.walcache.WalCacheManager;

public abstract class PackWalFactory {

  public abstract PackWalWriter createPackWalWriter(String name, PackMetaData metaData,
      WriteBlockMonitor writeBlockMonitor, BroadcastServerManager serverStatusManager) throws IOException;

  public abstract PackWalReader createPackWalReader(String name, PackMetaData metaData,
      WalCacheManager walCacheManager, MaxBlockLayer maxBlockLayer) throws IOException;

}
