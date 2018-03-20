package pack.distributed.storage.metrics.text;

import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.*;

/**
 * A reporter which outputs measurements to a {@link PrintStream}, like
 * {@code System.out}.
 */
public class TextReporter extends ScheduledReporter {
  /**
   * Returns a new {@link Builder} for {@link TextReporter}.
   *
   * @param registry
   *          the registry to report
   * @return a {@link Builder} instance for a {@link TextReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder for {@link TextReporter} instances. Defaults to using the
   * default locale and time zone, writing to {@code System.out}, converting
   * rates to events/second, converting durations to milliseconds, and not
   * filtering metrics.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private PrintStreamFactory printStreamFactory;
    private Locale locale;
    private Clock clock;
    private TimeZone timeZone;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private ScheduledExecutorService executor;
    private boolean shutdownExecutorOnStop;
    private Set<MetricAttribute> disabledMetricAttributes;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.printStreamFactory = () -> new PrintStream(System.out) {
        @Override
        public void close() {
          // Do nothing
        }
      };
      this.locale = Locale.getDefault();
      this.clock = Clock.defaultClock();
      this.timeZone = TimeZone.getDefault();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.executor = null;
      this.shutdownExecutorOnStop = true;
      disabledMetricAttributes = Collections.emptySet();
    }

    /**
     * Specifies whether or not, the executor (used for reporting) will be
     * stopped with same time with reporter. Default value is true. Setting this
     * parameter to false, has the sense in combining with providing external
     * managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
     *
     * @param shutdownExecutorOnStop
     *          if true, then executor will be stopped in same time with this
     *          reporter
     * @return {@code this}
     */
    public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
      this.shutdownExecutorOnStop = shutdownExecutorOnStop;
      return this;
    }

    /**
     * Specifies the executor to use while scheduling reporting of metrics.
     * Default value is null. Null value leads to executor will be auto created
     * on start.
     *
     * @param executor
     *          the executor to use while scheduling reporting of metrics.
     * @return {@code this}
     */
    public Builder scheduleOn(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    /**
     * Write to the given {@link PrintStream}.
     *
     * @param output
     *          a {@link PrintStream} instance.
     * @return {@code this}
     */
    public Builder outputTo(PrintStreamFactory printStreamFactory) {
      this.printStreamFactory = printStreamFactory;
      return this;
    }

    /**
     * Format numbers for the given {@link Locale}.
     *
     * @param locale
     *          a {@link Locale}
     * @return {@code this}
     */
    public Builder formattedFor(Locale locale) {
      this.locale = locale;
      return this;
    }

    /**
     * Use the given {@link Clock} instance for the time.
     *
     * @param clock
     *          a {@link Clock} instance
     * @return {@code this}
     */
    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    /**
     * Use the given {@link TimeZone} for the time.
     *
     * @param timeZone
     *          a {@link TimeZone}
     * @return {@code this}
     */
    public Builder formattedFor(TimeZone timeZone) {
      this.timeZone = timeZone;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit
     *          a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit
     *          a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter
     *          a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Don't report the passed metric attributes for all metrics (e.g. "p999",
     * "stddev" or "m15"). See {@link MetricAttribute}.
     *
     * @param disabledMetricAttributes
     *          a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
      this.disabledMetricAttributes = disabledMetricAttributes;
      return this;
    }

    /**
     * Builds a {@link TextReporter} with the given properties.
     *
     * @return a {@link TextReporter}
     */
    public TextReporter build() {
      return new TextReporter(registry, printStreamFactory, locale, clock, timeZone, rateUnit, durationUnit, filter,
          executor, shutdownExecutorOnStop, disabledMetricAttributes);
    }
  }

  private static final int CONSOLE_WIDTH = 80;

  private final PrintStreamFactory printStreamFactory;
  private final Locale locale;
  private final Clock clock;
  private final DateFormat dateFormat;

  private TextReporter(MetricRegistry registry, PrintStreamFactory printStreamFactory, Locale locale, Clock clock,
      TimeZone timeZone, TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter,
      ScheduledExecutorService executor, boolean shutdownExecutorOnStop,
      Set<MetricAttribute> disabledMetricAttributes) {
    super(registry, "console-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop,
        disabledMetricAttributes);
    this.printStreamFactory = printStreamFactory;
    this.locale = locale;
    this.clock = clock;
    this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, locale);
    dateFormat.setTimeZone(timeZone);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    final String dateTime = dateFormat.format(new Date(clock.getTime()));
    try (PrintStream output = printStreamFactory.newPrintStream()) {
      printWithBanner(output, dateTime, '=');
      output.println();

      if (!gauges.isEmpty()) {
        printWithBanner(output, "-- Gauges", '-');
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
          output.println(entry.getKey());
          printGauge(output, entry.getValue());
        }
        output.println();
      }

      if (!counters.isEmpty()) {
        printWithBanner(output, "-- Counters", '-');
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
          output.println(entry.getKey());
          printCounter(output, entry);
        }
        output.println();
      }

      if (!histograms.isEmpty()) {
        printWithBanner(output, "-- Histograms", '-');
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
          output.println(entry.getKey());
          printHistogram(output, entry.getValue());
        }
        output.println();
      }

      if (!meters.isEmpty()) {
        printWithBanner(output, "-- Meters", '-');
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
          output.println(entry.getKey());
          printMeter(output, entry.getValue());
        }
        output.println();
      }

      if (!timers.isEmpty()) {
        printWithBanner(output, "-- Timers", '-');
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
          output.println(entry.getKey());
          printTimer(output, entry.getValue());
        }
        output.println();
      }

      output.println();
      output.flush();
    }
  }

  private void printMeter(PrintStream output, Meter meter) {
    printIfEnabled(output, MetricAttribute.COUNT, String.format(locale, "             count = %d", meter.getCount()));
    printIfEnabled(output, MetricAttribute.MEAN_RATE,
        String.format(locale, "         mean rate = %2.2f events/%s", convertRate(meter.getMeanRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M1_RATE, String.format(locale, "     1-minute rate = %2.2f events/%s",
        convertRate(meter.getOneMinuteRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M5_RATE, String.format(locale, "     5-minute rate = %2.2f events/%s",
        convertRate(meter.getFiveMinuteRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M15_RATE, String.format(locale, "    15-minute rate = %2.2f events/%s",
        convertRate(meter.getFifteenMinuteRate()), getRateUnit()));
  }

  private void printCounter(PrintStream output, Map.Entry<String, Counter> entry) {
    output.printf(locale, "             count = %d%n", entry.getValue()
                                                            .getCount());
  }

  private void printGauge(PrintStream output, Gauge<?> gauge) {
    output.printf(locale, "             value = %s%n", gauge.getValue());
  }

  private void printHistogram(PrintStream output, Histogram histogram) {
    printIfEnabled(output, MetricAttribute.COUNT,
        String.format(locale, "             count = %d", histogram.getCount()));
    Snapshot snapshot = histogram.getSnapshot();
    printIfEnabled(output, MetricAttribute.MIN, String.format(locale, "               min = %d", snapshot.getMin()));
    printIfEnabled(output, MetricAttribute.MAX, String.format(locale, "               max = %d", snapshot.getMax()));
    printIfEnabled(output, MetricAttribute.MEAN,
        String.format(locale, "              mean = %2.2f", snapshot.getMean()));
    printIfEnabled(output, MetricAttribute.STDDEV,
        String.format(locale, "            stddev = %2.2f", snapshot.getStdDev()));
    printIfEnabled(output, MetricAttribute.P50,
        String.format(locale, "            median = %2.2f", snapshot.getMedian()));
    printIfEnabled(output, MetricAttribute.P75,
        String.format(locale, "              75%% <= %2.2f", snapshot.get75thPercentile()));
    printIfEnabled(output, MetricAttribute.P95,
        String.format(locale, "              95%% <= %2.2f", snapshot.get95thPercentile()));
    printIfEnabled(output, MetricAttribute.P98,
        String.format(locale, "              98%% <= %2.2f", snapshot.get98thPercentile()));
    printIfEnabled(output, MetricAttribute.P99,
        String.format(locale, "              99%% <= %2.2f", snapshot.get99thPercentile()));
    printIfEnabled(output, MetricAttribute.P999,
        String.format(locale, "            99.9%% <= %2.2f", snapshot.get999thPercentile()));
  }

  private void printTimer(PrintStream output, Timer timer) {
    final Snapshot snapshot = timer.getSnapshot();
    printIfEnabled(output, MetricAttribute.COUNT, String.format(locale, "             count = %d", timer.getCount()));
    printIfEnabled(output, MetricAttribute.MEAN_RATE,
        String.format(locale, "         mean rate = %2.2f calls/%s", convertRate(timer.getMeanRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M1_RATE, String.format(locale, "     1-minute rate = %2.2f calls/%s",
        convertRate(timer.getOneMinuteRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M5_RATE, String.format(locale, "     5-minute rate = %2.2f calls/%s",
        convertRate(timer.getFiveMinuteRate()), getRateUnit()));
    printIfEnabled(output, MetricAttribute.M15_RATE, String.format(locale, "    15-minute rate = %2.2f calls/%s",
        convertRate(timer.getFifteenMinuteRate()), getRateUnit()));

    printIfEnabled(output, MetricAttribute.MIN,
        String.format(locale, "               min = %2.2f %s", convertDuration(snapshot.getMin()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.MAX,
        String.format(locale, "               max = %2.2f %s", convertDuration(snapshot.getMax()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.MEAN,
        String.format(locale, "              mean = %2.2f %s", convertDuration(snapshot.getMean()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.STDDEV, String.format(locale, "            stddev = %2.2f %s",
        convertDuration(snapshot.getStdDev()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P50, String.format(locale, "            median = %2.2f %s",
        convertDuration(snapshot.getMedian()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P75, String.format(locale, "              75%% <= %2.2f %s",
        convertDuration(snapshot.get75thPercentile()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P95, String.format(locale, "              95%% <= %2.2f %s",
        convertDuration(snapshot.get95thPercentile()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P98, String.format(locale, "              98%% <= %2.2f %s",
        convertDuration(snapshot.get98thPercentile()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P99, String.format(locale, "              99%% <= %2.2f %s",
        convertDuration(snapshot.get99thPercentile()), getDurationUnit()));
    printIfEnabled(output, MetricAttribute.P999, String.format(locale, "            99.9%% <= %2.2f %s",
        convertDuration(snapshot.get999thPercentile()), getDurationUnit()));
  }

  private void printWithBanner(PrintStream output, String s, char c) {
    output.print(s);
    output.print(' ');
    for (int i = 0; i < (CONSOLE_WIDTH - s.length() - 1); i++) {
      output.print(c);
    }
    output.println();
  }

  /**
   * Print only if the attribute is enabled
   *
   * @param type
   *          Metric attribute
   * @param status
   *          Status to be logged
   */
  private void printIfEnabled(PrintStream output, MetricAttribute type, String status) {
    if (getDisabledMetricAttributes().contains(type)) {
      return;
    }

    output.println(status);
  }
}
