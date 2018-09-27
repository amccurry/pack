package pack.block.blockstore.crc;

import java.io.IOException;

public class CrcLayerFactory {

  public static CrcLayer create(String name, int entries, int blockSize) throws IOException {
    return () -> blockSize;
  }
}
