package pack.hdfs.storage.util;

import java.io.IOException;

public interface CommitFile {

  void commit() throws IOException;

}
