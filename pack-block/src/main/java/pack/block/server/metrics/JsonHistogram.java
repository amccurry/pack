package pack.block.server.metrics;

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
public class JsonHistogram {

  String name;
  long count;
  long max;
  double mean;
  long min;
  double stddev;
  double p50;
  double p75;
  double p95;
  double p98;
  double p99;
  double p999;

}
