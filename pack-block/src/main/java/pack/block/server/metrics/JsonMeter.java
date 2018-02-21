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
public class JsonMeter {
  String name;
  long count;
  double mean_rate;
  double m1_rate;
  double m5_rate;
  double m15_rate;
  String rate_unit;
}
