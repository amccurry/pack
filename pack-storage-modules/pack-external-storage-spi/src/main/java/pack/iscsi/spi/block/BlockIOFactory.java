package pack.iscsi.spi.block;

import java.io.IOException;

public interface BlockIOFactory {

  BlockIOExecutor getBlockWriter() throws IOException;

  BlockIOExecutor getBlockReader() throws IOException;

}
