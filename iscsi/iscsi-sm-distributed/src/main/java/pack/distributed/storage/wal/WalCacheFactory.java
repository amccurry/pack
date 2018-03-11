package pack.distributed.storage.wal;

import java.io.IOException;

public interface WalCacheFactory {

  WalCache create(long layer) throws IOException;

}
