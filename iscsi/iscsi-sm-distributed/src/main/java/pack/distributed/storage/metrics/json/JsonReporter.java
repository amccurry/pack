package pack.distributed.storage.metrics.json;

import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import pack.distributed.storage.http.Metric;
import pack.distributed.storage.metrics.json.JsonReport.JsonReportBuilder;

@SuppressWarnings("rawtypes")
public class JsonReporter extends ScheduledReporter {

  private final AtomicReference<JsonReport> _reportRef = new AtomicReference<JsonReport>(JsonReport.builder()
                                                                                                   .build());
  private final AtomicReference<List<Metric>> _metricRef = new AtomicReference<>(ImmutableList.of());

  public JsonReporter(MetricRegistry registry) {
    super(registry, "json-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
  }

  public JsonReport getReport() {
    return _reportRef.get();
  }

  public List<Metric> getMetricRef() {
    return _metricRef.get();
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    JsonReportBuilder reportBuilder = JsonReport.builder();
    {
      Builder<String, JsonGauge> builder = ImmutableMap.builder();
      for (Entry<String, Gauge> e : gauges.entrySet()) {
        builder.put(e.getKey(), JsonGauge.toJsonGauge(e.getValue()));
      }
      reportBuilder.gauges(builder.build());
    }
    {
      Builder<String, JsonCounter> builder = ImmutableMap.builder();
      for (Entry<String, Counter> e : counters.entrySet()) {
        builder.put(e.getKey(), JsonCounter.toJsonCounter(e.getValue()));
      }
      reportBuilder.counters(builder.build());
    }
    {
      Builder<String, JsonHistogram> builder = ImmutableMap.builder();
      for (Entry<String, Histogram> e : histograms.entrySet()) {
        builder.put(e.getKey(), JsonHistogram.toJsonHistogram(e.getValue()));
      }
      reportBuilder.histograms(builder.build());
    }
    {
      Builder<String, JsonMeter> builder = ImmutableMap.builder();
      for (Entry<String, Meter> e : meters.entrySet()) {
        builder.put(e.getKey(), JsonMeter.toJsonMeter(e.getValue()));
      }
      reportBuilder.meters(builder.build());
    }
    {
      Builder<String, JsonTimer> builder = ImmutableMap.builder();
      for (Entry<String, Timer> e : timers.entrySet()) {
        builder.put(e.getKey(), JsonTimer.toJsonTimer(e.getValue()));
      }
      reportBuilder.timers(builder.build());
    }
    JsonReport report = reportBuilder.build();
    _reportRef.set(report);
    _metricRef.set(Metric.flatten(report));
  }

}
