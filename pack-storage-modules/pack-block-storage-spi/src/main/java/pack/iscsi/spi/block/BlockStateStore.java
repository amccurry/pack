package pack.iscsi.spi.block;

import java.io.IOException;

public interface BlockStateStore {

  void setMaxBlockCount(long volumeId, long blockCount) throws IOException;

  void createBlockMetadataStore(long volumeId) throws IOException;

  void destroyBlockMetadataStore(long volumeId) throws IOException;

  BlockMetadata getBlockMetadata(long volumeId, long blockId) throws IOException;

  void setBlockMetadata(long volumeId, long blockId, BlockMetadata metadata) throws IOException;

  void removeBlockMetadata(long volumeId, long blockId) throws IOException;

}
