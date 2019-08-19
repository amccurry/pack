package pack.iscsi.manager;

import java.io.IOException;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

public interface TargetManager {

  Target getTarget(String targetName);

  String[] getTargetNames();

  boolean isValidTarget(String targetName);

  void register(String name, String alias, IStorageModule module) throws IOException;

  String getFullName(String name);
}
