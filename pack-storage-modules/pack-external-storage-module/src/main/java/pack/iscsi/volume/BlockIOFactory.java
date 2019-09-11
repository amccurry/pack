package pack.iscsi.volume;

import java.io.IOException;

import pack.iscsi.spi.block.BlockIOExecutor;

public interface BlockIOFactory {

  BlockIOExecutor getBlockWriter() throws IOException;

  BlockIOExecutor getBlockReader() throws IOException;

}
