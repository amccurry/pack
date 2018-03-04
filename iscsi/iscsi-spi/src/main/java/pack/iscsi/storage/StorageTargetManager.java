package pack.iscsi.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

public interface StorageTargetManager {

  Target getTarget(String targetName);

  String[] getTargetNames();

  boolean isValidTarget(String targetName);

  default void register(String name, String alias, IStorageModule module) throws IOException {
    throw new RuntimeException("not implemented");
  }

  default String getFullName(String name) {
    throw new RuntimeException("not implemented");
  }


  public static StorageTargetManager merge(StorageTargetManager... targetManagers) {
    return merge(Arrays.asList(targetManagers));
  }

  public static StorageTargetManager merge(List<StorageTargetManager> targetManagers) {
    return new StorageTargetManager() {

      @Override
      public Target getTarget(String targetName) {
        for (StorageTargetManager manager : targetManagers) {
          Target target = manager.getTarget(targetName);
          if (target != null) {
            return target;
          }
        }
        return null;
      }

      @Override
      public String[] getTargetNames() {
        List<String> names = new ArrayList<>();
        for (StorageTargetManager manager : targetManagers) {
          names.addAll(Arrays.asList(manager.getTargetNames()));
        }
        return names.toArray(new String[] {});
      }

      @Override
      public boolean isValidTarget(String targetName) {
        for (StorageTargetManager manager : targetManagers) {
          if (manager.isValidTarget(targetName)) {
            return true;
          }
        }
        return false;
      }
    };
  }

}
