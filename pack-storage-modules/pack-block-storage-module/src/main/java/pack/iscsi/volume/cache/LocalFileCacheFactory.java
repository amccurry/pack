package pack.iscsi.volume.cache;

import java.io.Closeable;

import pack.iscsi.spi.RandomAccessIO;

public interface LocalFileCacheFactory extends Closeable {

  RandomAccessIO getRandomAccessIO(long volumeId, long blockId);

}
