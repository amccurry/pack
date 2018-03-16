package pack.iscsi.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricsRegistrySingleton {

  private static MetricRegistry _registry;

  private MetricsRegistrySingleton() {
    
  }

  public static synchronized MetricRegistry getInstance() {
    if (_registry == null) {
      _registry = new MetricRegistry();
    }
    return _registry;
  }

}
