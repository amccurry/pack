package pack.iscsi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

public class BaseTargetManager implements TargetManager {

  private final String _targetPrefix;
  private final ConcurrentMap<String, Target> _targetMap = new ConcurrentHashMap<>();

  public BaseTargetManager(String date, String domain) {
    _targetPrefix = "iqn." + date + "." + domain;
  }

  @Override
  public Target getTarget(String targetName) {
    System.err.println("getTarget " + targetName);
    return _targetMap.get(targetName);
  }

  @Override
  public String[] getTargetNames() {
    List<String> list = new ArrayList<>(_targetMap.keySet());
    Collections.sort(list);
    return list.toArray(new String[list.size()]);
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
    return _targetPrefix + ":" + name;
  }

}
