package pack.iscsi.spi;

import java.io.IOException;
import java.util.Properties;

public abstract class StorageModuleLoader {

  private final String _type;

  protected StorageModuleLoader(String type) {
    _type = type;
  }

  public String getType() {
    return _type;
  }

  public abstract StorageModuleFactory create(String instanceName, Properties properties) throws IOException;

}
