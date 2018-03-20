package pack.distributed.storage.metrics.json;

import com.codahale.metrics.Counter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class JsonCounter {
  long count;

  public static JsonCounter toJsonCounter(Counter value) {
    return JsonCounter.builder()
                      .count(value.getCount())
                      .build();
  }

}
