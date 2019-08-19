package pack.iscsi.spi;

import java.io.IOException;
import java.util.List;

public interface StorageModuleFactory {

  List<String> getStorageModuleNames() throws IOException;

  StorageModule getStorageModule(String name) throws IOException;

}
