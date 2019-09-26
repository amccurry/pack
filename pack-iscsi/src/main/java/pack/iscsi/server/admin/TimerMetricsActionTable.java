package pack.iscsi.server.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import pack.iscsi.admin.ActionTable;
import pack.iscsi.admin.Column;
import pack.iscsi.admin.Row;
import pack.iscsi.spi.metric.MetricsFactory;

public class TimerMetricsActionTable extends ScheduledReporter implements ActionTable {

  private final AtomicReference<List<Row>> _rowsRef = new AtomicReference<>(new ArrayList<>());

  public TimerMetricsActionTable(MetricsFactory metricsFactory) {
    super(toMetricRegistry(metricsFactory.getMetricRegistry()), "actiontable-reporter", (name, metric) -> true,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    start(10, TimeUnit.SECONDS);
  }

  private static MetricRegistry toMetricRegistry(Object metricRegistry) {
    return (MetricRegistry) metricRegistry;
  }

  @Override
  public String getName() throws IOException {
    return "timers";
  }

  @Override
  public String getLink() throws IOException {
    return "timermetrics";
  }

  @Override
  public List<Row> getRows() throws IOException {
    return _rowsRef.get();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    List<Row> rows = new ArrayList<>();
    Set<Entry<String, Timer>> entrySet = timers.entrySet();
    for (Entry<String, Timer> entry : entrySet) {
      rows.add(Row.builder()
                  .columns(timerToColumns(entry))
                  .build());
    }
    _rowsRef.set(rows);
  }

  @Override
  public List<String> getHeaders() throws IOException {
    return Arrays.asList("Name", "Count", "75%", "95%", "98%", "99%","99.9%","Min","Max","Mean","StdDev");
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

  private List<Column> timerToColumns(Entry<String, Timer> entry) {
    String name = entry.getKey();
    Timer timer = entry.getValue();
    long count = timer.getCount();
    Snapshot snapshot = timer.getSnapshot();
    double get75thPercentile = snapshot.get75thPercentile();
    double get95thPercentile = snapshot.get95thPercentile();
    double get98thPercentile = snapshot.get98thPercentile();
    double get99thPercentile = snapshot.get99thPercentile();
    double get999thPercentile = snapshot.get999thPercentile();
    long min = snapshot.getMin();
    long max = snapshot.getMax();
    double mean = snapshot.getMean();
    double stdDev = snapshot.getStdDev();
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder()
                      .value(name)
                      .build());
    columns.add(Column.builder()
                      .value(toString(count))
                      .build());
    columns.add(Column.builder()
                      .value(toString(get75thPercentile))
                      .build());
    columns.add(Column.builder()
                      .value(toString(get95thPercentile))
                      .build());
    columns.add(Column.builder()
                      .value(toString(get98thPercentile))
                      .build());
    columns.add(Column.builder()
                      .value(toString(get99thPercentile))
                      .build());
    columns.add(Column.builder()
                      .value(toString(get999thPercentile))
                      .build());
    columns.add(Column.builder()
                      .value(toString(min))
                      .build());
    columns.add(Column.builder()
                      .value(toString(max))
                      .build());
    columns.add(Column.builder()
                      .value(toString(mean))
                      .build());
    columns.add(Column.builder()
                      .value(toString(stdDev))
                      .build());
    return columns;
  }
}
