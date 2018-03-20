package pack.distributed.storage.metrics.json;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JsonMeter {

  long count;
  double meanRate;
  double oneMinuteRate;
  double fiveMinuteRate;
  double fifteenMinuteRate;

  public static JsonMeter toJsonMeter(Meter meter) {
    return JsonMeter.builder()
                    .count(meter.getCount())
                    .meanRate(meter.getMeanRate())
                    .oneMinuteRate(meter.getOneMinuteRate())
                    .fiveMinuteRate(meter.getFiveMinuteRate())
                    .fifteenMinuteRate(meter.getFifteenMinuteRate())
                    .build();
  }

}
