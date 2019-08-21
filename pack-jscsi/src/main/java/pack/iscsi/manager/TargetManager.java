package pack.iscsi.manager;

import java.io.IOException;

import org.jscsi.target.Target;

import pack.iscsi.spi.StorageModuleFactory;

public interface TargetManager {

  Target getTarget(String targetName);

  String[] getTargetNames();

  boolean isValidTarget(String targetName);

  void register(StorageModuleFactory moduleFactory) throws IOException;

  String getTargetName(String name);

  String getTargetAlias(String targetName);

}
