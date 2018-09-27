package pack.block.blockstore.hdfs;

import java.io.IOException;
import java.util.List;

import pack.block.blockstore.hdfs.file.ReadRequest;

public interface ReadRequestHandler {

  boolean readBlocks(List<ReadRequest> requests) throws IOException;

  long getLayer();

}
