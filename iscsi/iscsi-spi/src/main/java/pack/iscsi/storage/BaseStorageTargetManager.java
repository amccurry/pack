package pack.iscsi.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStorageTargetManager implements StorageTargetManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseStorageTargetManager.class);

  protected static final String TARGET_PREFIX = "iqn.2018-02.pack";

  protected final ConcurrentMap<String, Target> _targetMap = new ConcurrentHashMap<>();

  private final StorageModuleDelegateFactory _factory;

  public BaseStorageTargetManager(StorageModuleDelegateFactory factory) {
    _factory = factory;
  }

  @Override
  public Target getTarget(String targetName) {
    return _targetMap.get(targetName);
  }

  @Override
  public String[] getTargetNames() {
    registerNewTargets();
    List<String> list = new ArrayList<>(_targetMap.keySet());
    Collections.sort(list);
    return list.toArray(new String[list.size()]);
  }

  protected synchronized void registerNewTargets() {
    List<String> names = getVolumeNames();
    for (String name : names) {
      String fullName = getFullName(name);
      if (!isValidTarget(fullName)) {
        try {
          IStorageModule module = createNewStorageModule(name);
          if (_factory != null) {
            register(name, name, _factory.delegate(name, module));
          } else {
            register(name, name, module);
          }
        } catch (IOException e) {
          LOGGER.error("Error creating new storage module " + name, e);
        }
      }
    }
  }

  @Override
  public boolean isValidTarget(String targetName) {
    return _targetMap.containsKey(targetName);
  }

  @Override
  public synchronized void register(String name, String alias, IStorageModule module) throws IOException {
    String fullName = getFullName(name);
    if (isValidTarget(fullName)) {
      throw new IOException("Already registered " + fullName);
    }
    Target target = new Target(fullName, alias, module);
    _targetMap.put(fullName, target);
  }

  @Override
  public String getFullName(String name) {
    return getTargetPrefix() + ":" + name;
  }

  protected String getTargetPrefix() {
    return TARGET_PREFIX + "." + getType();
  }

}
