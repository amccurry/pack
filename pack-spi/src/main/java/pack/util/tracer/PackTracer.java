package pack.util.tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tag;

/**
 * MockTracer makes it easy to test the semantics of OpenTracing
 * instrumentation.
 *
 * By using a MockTracer as an io.opentracing.Tracer implementation for
 * unittests, a developer can assert that Span properties and relationships with
 * other Spans are defined as expected by instrumentation code.
 *
 * The MockTracerTest has simple usage examples.
 */
public class PackTracer implements Tracer {
  private final ScopeManager _scopeManager;

  public PackTracer(ScopeManager scopeManager) {
    _scopeManager = scopeManager;
  }

  @Override
  public ScopeManager scopeManager() {
    return _scopeManager;
  }

  @Override
  public SpanBuilder buildSpan(String operationName) {
    return new SpanBuilder(operationName);
  }

  @Override
  public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public <C> SpanContext extract(Format<C> format, C carrier) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Span activeSpan() {
    return _scopeManager.activeSpan();
  }

  @Override
  public Scope activateSpan(Span span) {
    return _scopeManager.activate(span);
  }

  @Override
  public void close() {

  }

  private SpanContext activeSpanContext() {
    Span span = activeSpan();
    if (span == null) {
      return null;
    }

    return span.context();
  }

  public final class SpanBuilder implements Tracer.SpanBuilder {
    private final String _operationName;
    private long _startMicros;
    private List<PackSpan.Reference> _references = new ArrayList<>();
    private boolean _ignoringActiveSpan;
    private Map<String, Object> _initialTags = new HashMap<>();

    SpanBuilder(String operationName) {
      _operationName = operationName;
    }

    @Override
    public SpanBuilder asChildOf(SpanContext parent) {
      return addReference(References.CHILD_OF, parent);
    }

    @Override
    public SpanBuilder asChildOf(Span parent) {
      if (parent == null) {
        return this;
      }
      return addReference(References.CHILD_OF, parent.context());
    }

    @Override
    public SpanBuilder ignoreActiveSpan() {
      _ignoringActiveSpan = true;
      return this;
    }

    @Override
    public SpanBuilder addReference(String referenceType, SpanContext referencedContext) {
      if (referencedContext != null) {
        _references.add(new PackSpan.Reference((PackSpan.PackContext) referencedContext, referenceType));
      }
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, String value) {
      _initialTags.put(key, value);
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, boolean value) {
      _initialTags.put(key, value);
      return this;
    }

    @Override
    public SpanBuilder withTag(String key, Number value) {
      _initialTags.put(key, value);
      return this;
    }

    @Override
    public <T> Tracer.SpanBuilder withTag(Tag<T> tag, T value) {
      _initialTags.put(tag.getKey(), value);
      return this;
    }

    @Override
    public SpanBuilder withStartTimestamp(long microseconds) {
      _startMicros = microseconds;
      return this;
    }

    @Override
    public PackSpan start() {
      long startNanos;
      long startTsMicros;
      if (_startMicros == 0) {
        startNanos = System.nanoTime();
        startTsMicros = PackSpan.nowMicrosViaNanoTime();
      } else {
        startNanos = _startMicros * 1000;
        startTsMicros = _startMicros;
      }
      SpanContext activeSpanContext = activeSpanContext();
      if (_references.isEmpty() && !_ignoringActiveSpan && activeSpanContext != null) {
        _references.add(new PackSpan.Reference((PackSpan.PackContext) activeSpanContext, References.CHILD_OF));
      }
      return new PackSpan(_operationName, startTsMicros, startNanos, _initialTags, _references);
    }
  }
}
