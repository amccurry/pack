package pack.distributed.storage.trace;

import java.io.Closeable;

import org.slf4j.Logger;

public class PackTracer implements Closeable {

  public static PackTracer create(Logger logger, String name) {
    return new PackTracer(logger, name);
  }

  private final Logger _logger;
  private final long _start = System.nanoTime();
  private final String _name;

  private PackTracer(Logger logger, String name) {
    _logger = logger;
    _name = name;
  }

  public PackTracer span(Logger logger, String spanName) {
    return new PackTracer(logger, _name + "=>" + spanName);
  }

  @Override
  public void close() {
    long now = System.nanoTime();
    if (_logger.isTraceEnabled()) {
      _logger.trace("PackTracer {} took {} ms", _name, (now - _start) / 1_000_000.0);
    }
  }

}
