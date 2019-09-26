package pack.iscsi.spi;

import java.io.Closeable;
import java.io.IOException;

public interface RandomAccessIO extends Closeable, RandomAccessIOWriter, RandomAccessIOReader {

  RandomAccessIOReader cloneReadOnly() throws IOException;

}
