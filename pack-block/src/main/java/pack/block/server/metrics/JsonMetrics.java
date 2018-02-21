package pack.block.server.metrics;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonMetrics {

  List<JsonCounter> counters;
  List<JsonGauge> gauges;
  List<JsonHistogram> histograms;
  List<JsonMeter> meters;
  List<JsonTimer> timers;
  
}
