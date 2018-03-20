package pack.distributed.storage.metrics.json;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JsonReport {
  Map<String, JsonGauge> gauges;
  Map<String, JsonCounter> counters;
  Map<String, JsonHistogram> histograms;
  Map<String, JsonMeter> meters;
  Map<String, JsonTimer> timers;
}
