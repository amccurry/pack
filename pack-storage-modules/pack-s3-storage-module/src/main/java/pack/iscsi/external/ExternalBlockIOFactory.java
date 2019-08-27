package pack.iscsi.external;

import java.io.IOException;

import pack.iscsi.partitioned.block.BlockIOExecutor;

public interface ExternalBlockIOFactory {

  BlockIOExecutor getBlockWriter() throws IOException;

  BlockIOExecutor getBlockReader() throws IOException;

}
