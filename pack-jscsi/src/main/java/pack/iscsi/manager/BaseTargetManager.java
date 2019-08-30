package pack.iscsi.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.iscsi.spi.StorageModule;
import pack.iscsi.spi.StorageModuleFactory;

public class BaseTargetManager implements TargetManager {

  private static final String IQN_2019_08 = "iqn.2019-08.";
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTargetManager.class);
  private static final String PACK = "pack";
  private final String _targetPrefix;
  private final List<StorageModuleFactory> _factories;

  public BaseTargetManager(List<StorageModuleFactory> factories) {
    this(factories, null);
  }

  public BaseTargetManager(List<StorageModuleFactory> factories, String domain) {
    _factories = factories;
    if (domain == null) {
      _targetPrefix = IQN_2019_08 + PACK + ":";
    } else {
      _targetPrefix = IQN_2019_08 + domain + ":";
    }
  }

  @Override
  public Target getTarget(String targetName) {
    for (StorageModuleFactory factory : _factories) {
      try {
        String name = getModuleName(targetName);
        if (factory.getStorageModuleNames()
                   .contains(name)) {
          StorageModule storageModule = factory.getStorageModule(name);
          return new Target(targetName, PACK + " " + name, toIStorageModule(storageModule));
        }
      } catch (IOException e) {
        LOGGER.error("Unknown error getting module names", e);
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("Target " + targetName + " not found.");
  }

  @Override
  public String getTargetAlias(String targetName) {
    return PACK + " " + getModuleName(targetName);
  }

  private String getModuleName(String targetName) {
    if (targetName.startsWith(_targetPrefix)) {
      return targetName.substring(_targetPrefix.length());
    }
    return targetName;
  }

  @Override
  public String[] getTargetNames() {
    List<String> list = getTargetNameList();
    Collections.sort(list);
    return list.toArray(new String[list.size()]);
  }

  private List<String> getTargetNameList() {
    List<String> list = new ArrayList<>();
    for (StorageModuleFactory factory : _factories) {
      try {
        List<String> names = factory.getStorageModuleNames();
        for (String name : names) {
          list.add(getTargetName(name));
        }
      } catch (IOException e) {
        LOGGER.error("Unknown error getting module names", e);
        throw new RuntimeException(e);
      }
    }
    return list;
  }

  @Override
  public boolean isValidTarget(String targetName) {
    return getTargetNameList().contains(targetName);
  }

  @Override
  public String getTargetName(String name) {
    return _targetPrefix + name;
  }

  @Override
  public void register(StorageModuleFactory moduleFactory) throws IOException {
    _factories.add(moduleFactory);
  }

  private IStorageModule toIStorageModule(StorageModule storageModule) {
    return WrapperIStorageModule.create(storageModule);
  }

}
