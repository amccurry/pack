package pack.iscsi.partitioned.storagemanager;

import java.io.IOException;

import pack.iscsi.partitioned.block.BlockIOExecutor;

public interface BlockIOFactory {

  BlockIOExecutor getBlockWriter() throws IOException;

  BlockIOExecutor getBlockReader() throws IOException;

}
