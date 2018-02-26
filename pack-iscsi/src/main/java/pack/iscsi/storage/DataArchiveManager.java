package pack.iscsi.storage;

import java.io.Closeable;
import java.io.IOException;

public interface DataArchiveManager extends Closeable {

  BlockReader getBlockReader() throws IOException;

}
