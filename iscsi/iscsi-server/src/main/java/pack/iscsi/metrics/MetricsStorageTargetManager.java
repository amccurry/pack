package pack.iscsi.metrics;

import java.io.IOException;
import java.util.List;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

import com.codahale.metrics.MetricRegistry;

import pack.iscsi.storage.StorageTargetManager;

public class MetricsStorageTargetManager implements StorageTargetManager {

  private final StorageTargetManager _delegate;
  private final MetricRegistry _registry;

  public static StorageTargetManager wrap(MetricRegistry registry, StorageTargetManager manager) {
    return new MetricsStorageTargetManager(registry, manager);
  }

  public MetricsStorageTargetManager(MetricRegistry registry, StorageTargetManager manager) {
    _delegate = manager;
    _registry = registry;
  }

  @Override
  public Target getTarget(String targetName) {
    return _delegate.getTarget(targetName);
  }

  @Override
  public String[] getTargetNames() {
    return _delegate.getTargetNames();
  }

  @Override
  public boolean isValidTarget(String targetName) {
    return _delegate.isValidTarget(targetName);
  }

  @Override
  public void register(String name, String alias, IStorageModule module) throws IOException {
    _delegate.register(name, alias, module);
  }

  @Override
  public String getFullName(String name) {
    return _delegate.getFullName(name);
  }

  @Override
  public String getType() {
    return _delegate.getType();
  }

  @Override
  public IStorageModule createNewStorageModule(String name) throws IOException {
    return MetricsIStorageModule.wrap(name, _registry, _delegate.createNewStorageModule(name));
  }

  @Override
  public List<String> getVolumeNames() {
    return _delegate.getVolumeNames();
  }

}
