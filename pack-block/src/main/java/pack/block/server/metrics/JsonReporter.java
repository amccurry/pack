package pack.block.server.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import pack.block.server.metrics.JsonMetrics.JsonMetricsBuilder;

@SuppressWarnings("rawtypes")
public class JsonReporter extends ScheduledReporter {
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private Clock clock;
    private MetricFilter filter;
    private ScheduledExecutorService executor;
    private boolean shutdownExecutorOnStop;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.clock = Clock.defaultClock();
      this.filter = MetricFilter.ALL;
      this.executor = null;
      this.shutdownExecutorOnStop = true;
    }

    public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
      this.shutdownExecutorOnStop = shutdownExecutorOnStop;
      return this;
    }

    public Builder scheduleOn(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public JsonReporter build(Configuration configuration, Path directory) throws IOException {
      return new JsonReporter(registry, configuration, directory, rateUnit, durationUnit, clock, filter, executor,
          shutdownExecutorOnStop);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonReporter.class);

  private final Path directory;
  private final Clock clock;
  private final Configuration configuration;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private JsonReporter(MetricRegistry registry, Configuration configuration, Path directory, TimeUnit rateUnit,
      TimeUnit durationUnit, Clock clock, MetricFilter filter, ScheduledExecutorService executor,
      boolean shutdownExecutorOnStop) throws IOException {
    super(registry, "json-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
    this.configuration = configuration;
    this.directory = directory;
    this.clock = clock;
    FileSystem fileSystem = directory.getFileSystem(configuration);
    fileSystem.mkdirs(directory);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

    JsonMetricsBuilder builder = JsonMetrics.builder();

    ImmutableList.Builder<JsonGauge> gb = ImmutableList.builder();
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      gb.add(reportGauge(timestamp, entry.getKey(), entry.getValue()));
    }
    builder.gauges(gb.build());

    ImmutableList.Builder<JsonCounter> cb = ImmutableList.builder();
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      cb.add(reportCounter(timestamp, entry.getKey(), entry.getValue()));
    }
    builder.counters(cb.build());

    ImmutableList.Builder<JsonHistogram> hb = ImmutableList.builder();
    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      hb.add(reportHistogram(timestamp, entry.getKey(), entry.getValue()));
    }
    builder.histograms(hb.build());

    ImmutableList.Builder<JsonMeter> mb = ImmutableList.builder();
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      mb.add(reportMeter(timestamp, entry.getKey(), entry.getValue()));
    }
    builder.meters(mb.build());

    ImmutableList.Builder<JsonTimer> tb = ImmutableList.builder();
    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      tb.add(reportTimer(timestamp, entry.getKey(), entry.getValue()));
    }
    builder.timers(tb.build());

    try {
      Path path = new Path(directory, Long.toString(timestamp));
      FileSystem fileSystem = path.getFileSystem(configuration);
      try (FSDataOutputStream outputStream = fileSystem.create(path)) {
        objectMapper.writeValue((OutputStream) outputStream, builder.build());
      }
    } catch (IOException e) {
      LOGGER.error("Unknown error while trying to write mtrics.", e);
    }
  }

  private JsonTimer reportTimer(long timestamp, String name, Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();
    return JsonTimer.builder()
                    .name(name)
                    .count(timer.getCount())
                    .max(convertDuration(snapshot.getMax()))
                    .mean(convertDuration(snapshot.getMean()))
                    .min(convertDuration(snapshot.getMin()))
                    .stddev(convertDuration(snapshot.getStdDev()))
                    .p50(convertDuration(snapshot.getMedian()))
                    .p75(convertDuration(snapshot.get75thPercentile()))
                    .p95(convertDuration(snapshot.get95thPercentile()))
                    .p98(convertDuration(snapshot.get98thPercentile()))
                    .p99(convertDuration(snapshot.get99thPercentile()))
                    .p999(convertDuration(snapshot.get999thPercentile()))
                    .mean_rate(convertRate(timer.getMeanRate()))
                    .m1_rate(convertRate(timer.getOneMinuteRate()))
                    .m5_rate(convertRate(timer.getFiveMinuteRate()))
                    .m15_rate(convertRate(timer.getFifteenMinuteRate()))
                    .rate_unit(getRateUnit())
                    .duration_unit(getDurationUnit())
                    .build();

  }

  private JsonMeter reportMeter(long timestamp, String name, Meter meter) {
    return JsonMeter.builder()
                    .name(name)
                    .count(meter.getCount())
                    .mean_rate(convertRate(meter.getMeanRate()))
                    .m1_rate(convertRate(meter.getOneMinuteRate()))
                    .m5_rate(convertRate(meter.getFiveMinuteRate()))
                    .m15_rate(convertRate(meter.getFifteenMinuteRate()))
                    .rate_unit(getRateUnit())
                    .build();
  }

  private JsonHistogram reportHistogram(long timestamp, String name, Histogram histogram) {
    final Snapshot snapshot = histogram.getSnapshot();
    return JsonHistogram.builder()
                        .name(name)
                        .count(histogram.getCount())
                        .max(snapshot.getMax())
                        .mean(snapshot.getMean())
                        .min(snapshot.getMin())
                        .stddev(snapshot.getStdDev())
                        .p50(snapshot.getMedian())
                        .p75(snapshot.get75thPercentile())
                        .p95(snapshot.get95thPercentile())
                        .p98(snapshot.get98thPercentile())
                        .p99(snapshot.get99thPercentile())
                        .p999(snapshot.get999thPercentile())
                        .build();
  }

  private JsonCounter reportCounter(long timestamp, String name, Counter counter) {
    return JsonCounter.builder()
                      .name(name)
                      .count(counter.getCount())
                      .build();
  }

  private JsonGauge reportGauge(long timestamp, String name, Gauge gauge) {
    return JsonGauge.builder()
                    .name(name)
                    .value(gauge.getValue() == null ? null : gauge.getValue()
                                                                  .toString())
                    .build();
  }

  protected String sanitize(String name) {
    return name;
  }
}
