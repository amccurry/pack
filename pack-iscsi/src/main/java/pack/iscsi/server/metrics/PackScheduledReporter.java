package pack.iscsi.server.metrics;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import pack.iscsi.spi.metric.MetricsFactory;
import swa.spi.Column;
import swa.spi.Row;

public class PackScheduledReporter extends ScheduledReporter {

  private final AtomicReference<List<Row>> _rowsRef = new AtomicReference<>(new ArrayList<>());

  public PackScheduledReporter(MetricsFactory metricsFactory) {
    super(toMetricRegistry(metricsFactory.getMetricRegistry()), "pack-reporter", (name, metric) -> true,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    start(1, TimeUnit.MINUTES);
  }

  public List<Row> getRows() {
    return _rowsRef.get();
  }

  private static MetricRegistry toMetricRegistry(Object metricRegistry) {
    return (MetricRegistry) metricRegistry;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    List<Row> rows = new ArrayList<>();
    Set<Entry<String, Meter>> entrySet = meters.entrySet();
    for (Entry<String, Meter> entry : entrySet) {
      Row row = Row.builder()
                   .columns(meterToColumns(entry))
                   .build();
      rows.add(row);
    }
    _rowsRef.set(rows);
  }

  private List<Column> meterToColumns(Entry<String, Meter> entry) {
    String name = entry.getKey();
    Meter meter = entry.getValue();
    long count = meter.getCount();
    double meanRate = meter.getMeanRate();
    double oneMinuteRate = meter.getOneMinuteRate();
    double fiveMinuteRate = meter.getFiveMinuteRate();
    double fifteenMinuteRate = meter.getFifteenMinuteRate();
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder()
                      .value(name)
                      .build());
    columns.add(Column.builder()
                      .value(toString(count))
                      .build());
    columns.add(Column.builder()
                      .value(toString(meanRate))
                      .build());
    columns.add(Column.builder()
                      .value(toString(oneMinuteRate))
                      .build());
    columns.add(Column.builder()
                      .value(toString(fiveMinuteRate))
                      .build());
    columns.add(Column.builder()
                      .value(toString(fifteenMinuteRate))
                      .build());
    return columns;
  }

  private String toString(double d) {
    try (Formatter formatter = new Formatter()) {
      formatter.format("%,.0f", d);
      return formatter.out()
                      .toString();
    }
  }

  private String toString(long l) {
    try (Formatter formatter = new Formatter()) {
      formatter.format("%,d", l);
      return formatter.out()
                      .toString();
    }
  }
}
