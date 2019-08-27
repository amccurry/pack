package pack.iscsi.partitioned.block;

import java.io.IOException;

public interface BlockIOExecutor {

  BlockIOResponse exec(BlockIORequest request) throws IOException;

}
