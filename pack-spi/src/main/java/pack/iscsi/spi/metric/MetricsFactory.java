package pack.iscsi.spi.metric;

public interface MetricsFactory {

  public static MetricsFactory NO_OP = new MetricsFactory() {
    @Override
    public Meter meter(Class<?> clazz, String... name) {
      return count -> {
      };
    }

    @Override
    public TimerContext timer(Class<?> clazz, String... name) {
      return () -> () -> {
      };
    }
  };

  Meter meter(Class<?> clazz, String... name);

  TimerContext timer(Class<?> clazz, String... name);

  default Object getMetricRegistry() {
    return null;
  }

}
