package pack.util.tracer;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.tag.Tag;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public final class PackSpan implements Span {
  private static AtomicLong nextId = new AtomicLong(0);

  private PackContext _context;
  private final long _parentId; // 0 if there's no parent.
  private final long _startMicros;
  private boolean _finished;
  private long _finishMicros;
  private final Map<String, Object> _tags;
  private final List<LogEntry> _logEntries = new ArrayList<>();
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

  public long parentId() {
    return _parentId;
  }

  public long startMicros() {
    return _startMicros;
  }

  public long finishMicros() {
    assert _finishMicros > 0 : "must call finish() before finishMicros()";
    return _finishMicros;
  }

  public Map<String, Object> tags() {
    return new HashMap<>(_tags);
  }

  public List<LogEntry> logEntries() {
    return new ArrayList<>(_logEntries);
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
    finish(nowMicros());
  }

  @Override
  public synchronized void finish(long finishMicros) {
    finishedCheck("Finishing already finished span");
    _finishMicros = finishMicros;
    _finished = true;
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
    return log(nowMicros(), fields);
  }

  @Override
  public final synchronized PackSpan log(long timestampMicros, Map<String, ?> fields) {
    finishedCheck("Adding logs %s at %d to already finished span", fields, timestampMicros);
    _logEntries.add(new LogEntry(timestampMicros, fields));
    return this;
  }

  @Override
  public PackSpan log(String event) {
    return log(nowMicros(), event);
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
    private final long _traceId;
    private final Map<String, String> _baggage;
    private final long _spanId;

    public PackContext(long traceId, long spanId, Map<String, String> baggage) {
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

    public long traceId() {
      return _traceId;
    }

    public long spanId() {
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

  public static final class LogEntry {
    private final long _timestampMicros;
    private final Map<String, ?> _fields;

    public LogEntry(long timestampMicros, Map<String, ?> fields) {
      _timestampMicros = timestampMicros;
      _fields = fields;
    }

    public long timestampMicros() {
      return _timestampMicros;
    }

    public Map<String, ?> fields() {
      return _fields;
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

  public PackSpan(String operationName, long startMicros, Map<String, Object> initialTags, List<Reference> refs) {
    _operationName = operationName;
    _startMicros = startMicros;
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
      _context = new PackContext(nextId(), nextId(), new HashMap<String, String>());
      _parentId = 0;
    } else {
      // We're a child Span.
      _context = new PackContext(parent._traceId, nextId(), mergeBaggages(_references));
      _parentId = parent._spanId;
    }
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

  static long nowMicros() {
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
}
