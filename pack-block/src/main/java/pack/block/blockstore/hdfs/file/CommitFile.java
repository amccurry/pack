package pack.block.blockstore.hdfs.file;

import java.io.IOException;

public interface CommitFile {

  void commit() throws IOException;

}
