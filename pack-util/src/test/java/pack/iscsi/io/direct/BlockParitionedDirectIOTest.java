package pack.iscsi.io.direct;

import java.io.File;
import java.io.IOException;

import pack.iscsi.spi.RandomAccessIO;

public class BlockParitionedDirectIOTest extends DirectIOTest {

  @Override
  protected RandomAccessIO getRandomAccessIO(File file) throws IOException {
    return new BlockParitionedDirectIO(file, 128 * 1024);
  }

  @Override
  protected int getBufferSize() {
    return 128 * 1024;
  }

  @Override
  protected int getBlockCount() {
    return 100;
  }

}
