package pack.block.blockstore.hdfs;

import java.io.IOException;

import pack.block.blockstore.BlockStore;

public interface HdfsBlockStore extends BlockStore {
  HdfsMetaData getMetaData() throws IOException;
}
