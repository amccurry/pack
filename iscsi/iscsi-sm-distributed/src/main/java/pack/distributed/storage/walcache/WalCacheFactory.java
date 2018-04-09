package pack.distributed.storage.walcache;

import java.io.IOException;

public interface WalCacheFactory {

  WalCache create(long layer) throws IOException;

}
