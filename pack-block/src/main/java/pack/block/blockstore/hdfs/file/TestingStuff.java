package pack.block.blockstore.hdfs.file;

import org.roaringbitmap.RoaringBitmap;

public class TestingStuff {

  public static void main(String[] args) {
    RoaringBitmap blocks = new RoaringBitmap();
    blocks.add(100);
    // System.out.println(blocks.select(100));
    System.out.println(blocks.rank(100));
  }

}
