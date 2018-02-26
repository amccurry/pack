package pack.iscsi.storage;

import java.io.IOException;

public interface DataArchiveManager {

  BlockReader getBlockReader() throws IOException;

}
