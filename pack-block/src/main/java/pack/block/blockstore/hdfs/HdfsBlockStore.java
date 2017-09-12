package pack.block.blockstore.hdfs;

import pack.block.blockstore.BlockStore;

public interface HdfsBlockStore extends BlockStore {
  HdfsMetaData getMetaData();
}
