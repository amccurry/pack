package pack.distributed.storage.hdfs;

import java.io.IOException;

public interface CommitFile {

  void commit() throws IOException;

}
