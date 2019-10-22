package pack.iscsi.server.admin;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import pack.iscsi.server.metrics.BucketMeter;
import pack.iscsi.server.metrics.BucketMeter.BucketEntry;
import pack.iscsi.spi.PackVolumeMetadata;
import pack.iscsi.spi.PackVolumeStore;
import pack.iscsi.spi.metric.Meter;
import pack.iscsi.spi.metric.MetricsFactory;
import swa.spi.ChartDataset;
import swa.spi.ChartElement;
import swa.spi.Column;
import swa.spi.Row;
import swa.spi.Table;

public class MetricsTable implements PackHtml, Table, MetricsFactory {

  private static final String BYTES = "bytes";
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsTable.class);
  private static final char SEPARATOR = '|';
  private static final Joiner JOINER = Joiner.on(SEPARATOR);

  private final LoadingCache<String, BucketMeter> _cache;
  private final int _maxBuckets;
  private final long _bucketSpan;
  private final TimeUnit _bucketSpanUnit;

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class RowData {
    List<Long> bucketTsList;
    Map<String, BucketEntry[]> valueMap;
  }

  private final AtomicReference<RowData> _reportData = new AtomicReference<>();
  private final Timer _timer;
  private final Timer _tick;
  private final PackVolumeStore _store;

  public MetricsTable(int maxBuckets, long bucketSpan, TimeUnit bucketSpanUnit, PackVolumeStore store) {
    _store = store;
    _maxBuckets = maxBuckets;
    _bucketSpan = bucketSpan;
    _bucketSpanUnit = bucketSpanUnit;
    CacheLoader<String, BucketMeter> loader = key -> new BucketMeter(_maxBuckets, _bucketSpan, _bucketSpanUnit);
    _cache = Caffeine.newBuilder()
                     .build(loader);
    gatherReportData();
    _timer = new Timer("metric-report", true);
    long period = bucketSpanUnit.toMillis(bucketSpan);
    _timer.scheduleAtFixedRate(getTask(), period, period);
    _tick = new Timer("metric-tick", true);
    _tick.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        ConcurrentMap<String, BucketMeter> map = _cache.asMap();
        for (Entry<String, BucketMeter> entry : map.entrySet()) {
          BucketMeter bucketMeter = entry.getValue();
          if (bucketMeter != null) {
            bucketMeter.mark(0);
          }
        }
      }
    }, TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(1));
  }

  @Override
  public String getName() throws IOException {
    return "Volume Metrics";
  }

  @Override
  public String getLinkName() throws IOException {
    return "metermetrics";
  }

  @Override
  public String getIcon() {
    return "bar-chart-2";
  }

  public List<ChartElement> getChartElements(String volumeName) throws IOException {
    RowData rowData = _reportData.get();

    PackVolumeMetadata volumeMetadata = _store.getVolumeMetadata(volumeName);
    long volumeId = volumeMetadata.getVolumeId();

    Map<String, List<String>> keysForVolume = getKeysForVolume(rowData.valueMap, Long.toString(volumeId));
    List<ChartElement> chartElements = new ArrayList<>();

    for (Entry<String, List<String>> entry : keysForVolume.entrySet()) {
      String type = entry.getKey();

      List<String> chartLabels = new ArrayList<>();
      createHeadersFromRowData(rowData, chartLabels);

      List<String> sourceKeys = entry.getValue();
      List<ChartDataset> datasets = new ArrayList<>();
      for (String key : sourceKeys) {
        String subType = getSubType(key);
        BucketEntry[] entries = rowData.getValueMap()
                                       .get(key);
        List<Number> values = new ArrayList<>();
        for (BucketEntry bucketEntry : entries) {
          if (bucketEntry != null) {
            if (type.contains(BYTES)) {
              values.add(toMb(getCountPerSecond(bucketEntry.getCount())));
            } else {
              values.add(getCountPerSecond(bucketEntry.getCount()));
            }
          } else {
            values.add(0);
          }
        }
        datasets.add(ChartDataset.builder()
                                 .label(subType + " " + type + (type.contains(BYTES) ? " MiB/s" : ""))
                                 .values(values)
                                 .color(getColor(subType))
                                 .build());
      }
      chartElements.add(ChartElement.builder()
                                    .chartLabels(chartLabels)
                                    .datasets(datasets)
                                    .build());
    }

    return chartElements;
  }

  private String getColor(String subType) {
    switch (subType) {
    case "write":
      return "red";
    case "read":
      return "green";
    default:
      return "gray";
    }
  }

  private double getCountPerSecond(long count) {
    return (double) count / (double) _bucketSpanUnit.toSeconds(_bucketSpan);
  }

  private String getSubType(String key) {
    List<String> list = Splitter.on(SEPARATOR)
                                .splitToList(key);
    return list.get(2);
  }

  private Number toMb(double count) {
    return ((double) count) / 1024 / 1024;
  }

  private Map<String, List<String>> getKeysForVolume(Map<String, BucketEntry[]> valueMap, String volumeName) {
    Map<String, List<String>> result = new HashMap<>();
    for (String key : valueMap.keySet()) {
      if (key.startsWith(volumeName + SEPARATOR)) {
        List<String> list = Splitter.on(SEPARATOR)
                                    .splitToList(key);
        String type = list.get(1);
        List<String> keys = result.get(type);
        if (keys == null) {
          result.put(type, keys = new ArrayList<>());
        }
        keys.add(key);
      }
    }
    return result;
  }

  @Override
  public List<Row> getRows(Map<String, String[]> queryParams) throws IOException {
    RowData rowData = _reportData.get();

    Map<String, BucketEntry[]> valueMap = rowData.getValueMap();
    List<String> metricNames = new ArrayList<>(valueMap.keySet());
    Collections.sort(metricNames);
    List<Row> result = new ArrayList<>();
    List<Long> bucketTsList = rowData.getBucketTsList();
    for (String name : metricNames) {
      BucketEntry[] bucketEntries = valueMap.get(name);
      List<Column> columns = new ArrayList<>();
      columns.add(Column.builder()
                        .value(name)
                        .build());
      OUTER: for (Long ts : bucketTsList) {
        long timestamp = ts;
        for (BucketEntry entry : bucketEntries) {
          if (entry.getTsMillis() == timestamp) {
            columns.add(Column.builder()
                              .value(Long.toString(entry.getCount()))
                              .build());
            continue OUTER;
          }
        }
        columns.add(Column.builder()
                          .build());
      }

      result.add(Row.builder()
                    .columns(columns)
                    .build());
    }
    return result;
  }

  @Override
  public List<String> getHeaders(Map<String, String[]> queryParams) throws IOException {
    RowData rowData = _reportData.get();
    List<String> headers = new ArrayList<>();
    headers.add("Name");
    createHeadersFromRowData(rowData, headers);
    return headers;
  }

  private void createHeadersFromRowData(RowData rowData, List<String> headers) {
    List<Long> bucketTsList = rowData.getBucketTsList();
    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
    for (Long ts : bucketTsList) {
      headers.add(format.format(new Date(ts)));
    }
  }

  @Override
  public Meter meter(Class<?> clazz, String... names) {
    String name = getName(clazz, names);
    return _cache.get(name);
  }

  private static String getName(Class<?> clazz, String... names) {
    return JOINER.join(names) + SEPARATOR + clazz.getSimpleName();
  }

  private TimerTask getTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          gatherReportData();
        } catch (Exception e) {
          LOGGER.error("Unknown error", e);
        }
      }
    };
  }

  private void gatherReportData() {
    ImmutableMap<String, BucketMeter> map = ImmutableMap.copyOf(_cache.asMap());
    ImmutableSet<Entry<String, BucketMeter>> set = map.entrySet();
    Set<Long> bucketTsSet = new TreeSet<>();
    Map<String, BucketEntry[]> valueMap = new HashMap<>();
    for (Entry<String, BucketMeter> entry : set) {
      String name = entry.getKey();
      BucketMeter meter = entry.getValue();
      if (meter != null) {
        BucketEntry[] entries = meter.getEntries();
        for (BucketEntry bucketEntry : entries) {
          if (bucketEntry != null) {
            bucketTsSet.add(bucketEntry.getTsMillis());
          }
        }
        valueMap.put(name, entries);
      }
    }

    List<Long> bucketTsList = new ArrayList<>(bucketTsSet);
    Collections.reverse(bucketTsList);
    _reportData.set(RowData.builder()
                           .bucketTsList(bucketTsList)
                           .valueMap(valueMap)
                           .build());
  }
}
