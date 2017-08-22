package pack.block.fuse;

import java.io.File;
import java.io.IOException;

import pack.block.blockstore.file.FileBlockStore;

public class UsingFS {

  public static void main(String[] args) throws IOException {
    try (FuseFS memfs = new FuseFS("./mnt")) {
      memfs.addBlockStore(new FileBlockStore(new File("data/data1")));
      memfs.localMount();
    }
  }

}
