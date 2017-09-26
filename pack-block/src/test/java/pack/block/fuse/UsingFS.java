package pack.block.fuse;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import pack.block.blockstore.file.FileBlockStore;

public class UsingFS {

  public static void main(String[] args) throws IOException {

    try (RandomAccessFile rand = new RandomAccessFile(new File("test.file"), "rw")) {
      rand.setLength(100_000_000);
    }

    // try (FuseFileSystem memfs = new FuseFileSystem("./mnt")) {
    // memfs.addBlockStore(new FileBlockStore(new File("data/data1"), true));
    // memfs.localMount();
    // }
  }

}
