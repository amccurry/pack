package pack.distributed.storage.metrics.json;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JsonHistogram {

  long count;
  long min;
  long max;
  double mean;
  double stdDev;
  double median;
  double p75th;
  double p95th;
  double p98th;
  double p99th;
  double p999th;

  public static JsonHistogram toJsonHistogram(Histogram histogram) {
    Snapshot snapshot = histogram.getSnapshot();
    return JsonHistogram.builder()
                        .count(histogram.getCount())
                        .min(snapshot.getMin())
                        .max(snapshot.getMax())
                        .mean(snapshot.getMean())
                        .stdDev(snapshot.getStdDev())
                        .median(snapshot.getMedian())
                        .p75th(snapshot.get75thPercentile())
                        .p95th(snapshot.get95thPercentile())
                        .p98th(snapshot.get98thPercentile())
                        .p99th(snapshot.get99thPercentile())
                        .p999th(snapshot.get999thPercentile())
                        .build();
  }

}
