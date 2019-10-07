package pack.util.tracer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Tag {

  public static enum Type {
    STRING, NUMBER, BOOLEAN
  }

  String name;
  Object value;
  Type type;

  public static Tag create(String name, Number value) {
    return Tag.builder()
              .name(name)
              .value(value)
              .type(Type.NUMBER)
              .build();
  }

  public static Tag create(String name, String value) {
    return Tag.builder()
              .name(name)
              .value(value)
              .type(Type.STRING)
              .build();
  }

  public static Tag create(String name, boolean value) {
    return Tag.builder()
              .name(name)
              .value(value)
              .type(Type.BOOLEAN)
              .build();
  }
}
