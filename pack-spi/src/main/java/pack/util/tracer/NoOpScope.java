package pack.util.tracer;

import io.opentracing.Scope;

public class NoOpScope implements Scope {

  @Override
  public void close() {

  }

}
