package pack.iscsi.volume.cache;

import java.io.Closeable;
import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;

public interface LocalFileCacheFactory extends Closeable {

  RandomAccessIO getRandomAccessIO(long volumeId, long blockId) throws IOException;

}
