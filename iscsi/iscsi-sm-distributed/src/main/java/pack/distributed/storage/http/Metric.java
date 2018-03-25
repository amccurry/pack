package pack.distributed.storage.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.distributed.storage.metrics.json.JsonCounter;
import pack.distributed.storage.metrics.json.JsonGauge;
import pack.distributed.storage.metrics.json.JsonHistogram;
import pack.distributed.storage.metrics.json.JsonMeter;
import pack.distributed.storage.metrics.json.JsonReport;
import pack.distributed.storage.metrics.json.JsonTimer;

@Value
@AllArgsConstructor
@Builder
public class Metric implements Comparable<Metric> {

  public static final String TIMER = "timer";
  public static final String METER = "meter";
  public static final String GUAGE = "guage";
  public static final String HISTOGRAM = "histogram";
  public static final String COUNTER = "counter";

  String name;
  String type;
  Object value;
  Long count;
  Long min;
  Long max;
  Double mean;
  Double stdDev;
  Double median;
  Double p75th;
  Double p95th;
  Double p98th;
  Double p99th;
  Double p999th;
  Double meanRate;
  Double oneMinuteRate;
  Double fiveMinuteRate;
  Double fifteenMinuteRate;

  public static List<Metric> flatten(JsonReport report) {

    List<Metric> list = new ArrayList<Metric>();

    Map<String, JsonCounter> counters = report.getCounters();
    if (counters != null) {
      for (Entry<String, JsonCounter> e : counters.entrySet()) {
        String name = e.getKey();
        JsonCounter jsonCounter = e.getValue();
        list.add(Metric.builder()
                       .type(COUNTER)
                       .name(name)
                       .count(jsonCounter.getCount())
                       .build());
      }
    }

    Map<String, JsonGauge> gauges = report.getGauges();
    if (gauges != null) {
      for (Entry<String, JsonGauge> e : gauges.entrySet()) {
        String name = e.getKey();
        JsonGauge jsonGauge = e.getValue();
        list.add(Metric.builder()
                       .type(GUAGE)
                       .name(name)
                       .value(jsonGauge.getValue())
                       .build());
      }
    }

    Map<String, JsonHistogram> histograms = report.getHistograms();
    if (histograms != null) {
      for (Entry<String, JsonHistogram> e : histograms.entrySet()) {
        String name = e.getKey();
        JsonHistogram jsonHistogram = e.getValue();
        list.add(Metric.builder()
                       .type(HISTOGRAM)
                       .name(name)
                       .count(jsonHistogram.getCount())
                       .max(jsonHistogram.getMax())
                       .mean(jsonHistogram.getMean())
                       .median(jsonHistogram.getMedian())
                       .min(jsonHistogram.getMin())
                       .p75th(jsonHistogram.getP75th())
                       .p95th(jsonHistogram.getP95th())
                       .p98th(jsonHistogram.getP98th())
                       .p99th(jsonHistogram.getP99th())
                       .p999th(jsonHistogram.getP999th())
                       .stdDev(jsonHistogram.getStdDev())
                       .build());
      }
    }

    Map<String, JsonMeter> meters = report.getMeters();
    if (meters != null) {
      for (Entry<String, JsonMeter> e : meters.entrySet()) {
        String name = e.getKey();
        JsonMeter jsonMeter = e.getValue();
        list.add(Metric.builder()
                       .type(METER)
                       .name(name)
                       .count(jsonMeter.getCount())
                       .meanRate(jsonMeter.getMeanRate())
                       .oneMinuteRate(jsonMeter.getOneMinuteRate())
                       .fiveMinuteRate(jsonMeter.getFiveMinuteRate())
                       .fifteenMinuteRate(jsonMeter.getFifteenMinuteRate())
                       .build());
      }
    }

    Map<String, JsonTimer> timers = report.getTimers();
    if (timers != null) {
      for (Entry<String, JsonTimer> e : timers.entrySet()) {
        String name = e.getKey();
        JsonTimer jsonTimer = e.getValue();
        list.add(Metric.builder()
                       .type(TIMER)
                       .name(name)
                       .count(jsonTimer.getCount())
                       .meanRate(jsonTimer.getMeanRate())
                       .oneMinuteRate(jsonTimer.getOneMinuteRate())
                       .fiveMinuteRate(jsonTimer.getFiveMinuteRate())
                       .fifteenMinuteRate(jsonTimer.getFifteenMinuteRate())
                       .max(jsonTimer.getMax())
                       .mean(jsonTimer.getMean())
                       .median(jsonTimer.getMedian())
                       .min(jsonTimer.getMin())
                       .p75th(jsonTimer.getP75th())
                       .p95th(jsonTimer.getP95th())
                       .p98th(jsonTimer.getP98th())
                       .p99th(jsonTimer.getP99th())
                       .p999th(jsonTimer.getP999th())
                       .stdDev(jsonTimer.getStdDev())
                       .build());
      }
    }

    Collections.sort(list);
    return list;
  }

  @Override
  public int compareTo(Metric o) {
    int compare = getName().compareTo(o.getName());
    if (compare == 0) {
      return getType().compareTo(o.getType());
    }
    return compare;
  }

}
