package pack.distributed.storage.metrics.json;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JsonGauge {

  Object value;

  @SuppressWarnings("rawtypes")
  public static JsonGauge toJsonGauge(Gauge value) {
    return JsonGauge.builder()
                    .value(value.getValue())
                    .build();
  }
}
