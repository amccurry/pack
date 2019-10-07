package pack.util.tracer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.tag.Tag;

public final class PackSpan implements Span {
  private static AtomicLong nextId = new AtomicLong(0);

  private PackContext _context;
  private final UUID _parentId; // 0 if there's no parent.
  private final long _startNanos;
  private boolean _finished;
  private long _finishNanos;
  private final Map<String, Object> _tags;
  private String _operationName;
  private final List<Reference> _references;
  private final List<RuntimeException> errors = new ArrayList<>();

  public String operationName() {
    return _operationName;
  }

  @Override
  public PackSpan setOperationName(String operationName) {
    finishedCheck("Setting operationName {%s} on already finished span", operationName);
    _operationName = operationName;
    return this;
  }

  public Map<String, Object> tags() {
    return new HashMap<>(_tags);
  }

  public List<RuntimeException> generatedErrors() {
    return new ArrayList<>(errors);
  }

  public List<Reference> references() {
    return new ArrayList<>(_references);
  }

  @Override
  public synchronized PackContext context() {
    return _context;
  }

  @Override
  public void finish() {
    finishedCheck("Finishing already finished span");
    _finishNanos = System.nanoTime();
    _finished = true;
    LoggerReporter.finish(nowMicrosViaMillisTime(), this);
  }

  @Override
  public synchronized void finish(long finishMicros) {
    finishedCheck("Finishing already finished span");
    _finishNanos = finishMicros * 1000;
    _finished = true;
    LoggerReporter.finish(finishMicros, this);
  }

  @Override
  public PackSpan setTag(String key, String value) {
    return setObjectTag(key, value);
  }

  @Override
  public PackSpan setTag(String key, boolean value) {
    return setObjectTag(key, value);
  }

  @Override
  public PackSpan setTag(String key, Number value) {
    return setObjectTag(key, value);
  }

  @Override
  public <T> PackSpan setTag(Tag<T> tag, T value) {
    tag.set(this, value);
    return this;
  }

  private synchronized PackSpan setObjectTag(String key, Object value) {
    finishedCheck("Adding tag {%s:%s} to already finished span", key, value);
    _tags.put(key, value);
    return this;
  }

  @Override
  public final Span log(Map<String, ?> fields) {
    return log(nowMicrosViaMillisTime(), fields);
  }

  @Override
  public final synchronized PackSpan log(long timestampMicros, Map<String, ?> fields) {
    finishedCheck("Adding logs %s at %d to already finished span", fields, timestampMicros);
    LoggerReporter.log(timestampMicros, this, fields);
    return this;
  }

  @Override
  public PackSpan log(String event) {
    return log(nowMicrosViaMillisTime(), event);
  }

  @Override
  public PackSpan log(long timestampMicroseconds, String event) {
    return log(timestampMicroseconds, Collections.singletonMap("event", event));
  }

  @Override
  public synchronized Span setBaggageItem(String key, String value) {
    finishedCheck("Adding baggage {%s:%s} to already finished span", key, value);
    _context = _context.withBaggageItem(key, value);
    return this;
  }

  @Override
  public synchronized String getBaggageItem(String key) {
    return _context.getBaggageItem(key);
  }

  public static final class PackContext implements SpanContext {
    private final UUID _traceId;
    private final Map<String, String> _baggage;
    private final UUID _spanId;

    public PackContext(UUID traceId, UUID spanId, Map<String, String> baggage) {
      _baggage = baggage;
      _traceId = traceId;
      _spanId = spanId;
    }

    public String getBaggageItem(String key) {
      return _baggage.get(key);
    }

    public String toTraceId() {
      return String.valueOf(_traceId);
    }

    public String toSpanId() {
      return String.valueOf(_spanId);
    }

    public UUID traceId() {
      return _traceId;
    }

    public UUID spanId() {
      return _spanId;
    }

    public PackContext withBaggageItem(String key, String val) {
      Map<String, String> newBaggage = new HashMap<>(_baggage);
      newBaggage.put(key, val);
      return new PackContext(_traceId, _spanId, newBaggage);
    }

    @Override
    public Iterable<Map.Entry<String, String>> baggageItems() {
      return _baggage.entrySet();
    }
  }

  public static final class Reference {
    private final PackContext _context;
    private final String _referenceType;

    public Reference(PackContext context, String referenceType) {
      _context = context;
      _referenceType = referenceType;
    }

    public PackContext getContext() {
      return _context;
    }

    public String getReferenceType() {
      return _referenceType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Reference reference = (Reference) o;
      return Objects.equals(_context, reference._context) && Objects.equals(_referenceType, reference._referenceType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_context, _referenceType);
    }
  }

  public PackSpan(String operationName, long startTsMicros, long startNanos, Map<String, Object> initialTags,
      List<Reference> refs) {
    _operationName = operationName;
    _startNanos = startNanos;
    if (initialTags == null) {
      _tags = new HashMap<>();
    } else {
      _tags = new HashMap<>(initialTags);
    }
    if (refs == null) {
      _references = Collections.emptyList();
    } else {
      _references = new ArrayList<>(refs);
    }
    PackContext parent = findPreferredParentRef(_references);
    if (parent == null) {
      // We're a root Span.
      _context = new PackContext(UUID.randomUUID(), UUID.randomUUID(), new HashMap<String, String>());
      _parentId = null;
    } else {
      // We're a child Span.
      _context = new PackContext(parent._traceId, UUID.randomUUID(), mergeBaggages(_references));
      _parentId = parent._spanId;
    }
    LoggerReporter.start(startTsMicros, this);
  }

  private static PackContext findPreferredParentRef(List<Reference> references) {
    if (references.isEmpty()) {
      return null;
    }
    for (Reference reference : references) {
      if (References.CHILD_OF.equals(reference.getReferenceType())) {
        return reference.getContext();
      }
    }
    return references.get(0)
                     .getContext();
  }

  private static Map<String, String> mergeBaggages(List<Reference> references) {
    Map<String, String> baggage = new HashMap<>();
    for (Reference ref : references) {
      if (ref.getContext()._baggage != null) {
        baggage.putAll(ref.getContext()._baggage);
      }
    }
    return baggage;
  }

  static long nextId() {
    return nextId.addAndGet(1);
  }

  static long nowMicrosViaNanoTime() {
    return System.nanoTime() / 1000;
  }

  static long nowMicrosViaMillisTime() {
    return System.currentTimeMillis() * 1000;
  }

  private synchronized void finishedCheck(String format, Object... args) {
    if (_finished) {
      RuntimeException ex = new IllegalStateException(String.format(format, args));
      errors.add(ex);
      throw ex;
    }
  }

  @Override
  public String toString() {
    return "{" + "traceId:" + _context.traceId() + ", spanId:" + _context.spanId() + ", parentId:" + _parentId
        + ", operationName:\"" + _operationName + "\"}";
  }

  public long elapsedMicros() {
    if (_finishNanos == 0) {
      return 0;
    }
    return (_finishNanos - _startNanos) / 1000;
  }
}
