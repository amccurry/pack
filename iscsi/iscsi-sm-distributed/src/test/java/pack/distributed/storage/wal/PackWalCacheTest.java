package pack.distributed.storage.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import pack.distributed.storage.read.ReadRequest;
import pack.iscsi.storage.utils.PackUtils;

public class PackWalCacheTest {

  private File _dirFile;

  @Before
  public void setup() {
    _dirFile = new File("./target/tmp/WalCacheTest");
    PackUtils.rmr(_dirFile);
    _dirFile.mkdirs();
  }

  @Test
  public void testWalCache() throws IOException {
    Random random = new Random();
    long id = random.nextInt(Integer.MAX_VALUE);
    int blockSize = random.nextInt(4000);
    int maxBlocks = random.nextInt(1000000);
    long length = (long) maxBlocks * (long) blockSize;
    int numberOfWrites = random.nextInt(1000);

    try (PackWalCache cache = new PackWalCache(_dirFile, id, length, blockSize)) {
      long layer = random.nextInt(Integer.MAX_VALUE);
      for (int i = 0; i < numberOfWrites; i++) {
        int blockId = random.nextInt(maxBlocks);
        byte[] buf = new byte[blockSize];
        random.nextBytes(buf);
        cache.write(layer, blockId, buf);

        ByteBuffer dest = ByteBuffer.allocate(blockSize);
        cache.readBlocks(Arrays.asList(new ReadRequest(blockId, 0, dest)));
        assertTrue(Arrays.equals(buf, dest.array()));
        layer++;
      }
      assertEquals(layer - 1, cache.getMaxLayer());
    }
  }

}
