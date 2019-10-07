package pack.util.tracer;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import pack.util.tracer.PackSpan.PackContext;
import pack.util.tracer.PackSpan.Reference;

public class LoggerReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger("TRACER");
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  public static void start(long timestampMicros, PackSpan span) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(toStructuredMessage(timestampMicros, "start", span, null));
    }
  }

  public static void finish(long timestampMicros, PackSpan span) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(toStructuredMessage(timestampMicros, "finish", span, null));
    }
  }

  public static void log(long timestampMicros, PackSpan span, Map<String, ?> fields) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(toStructuredMessage(timestampMicros, "log", span, fields));
    }
  }

  protected static String toStructuredMessage(long timestampMicros, String action, PackSpan span,
      Map<String, ?> fields) {
    try {
      StringWriter w = new StringWriter();
      JsonGenerator g = JSON_FACTORY.createGenerator(w);

      PackContext context = span.context();

      g.writeStartObject();
      g.writeNumberField("ts", timestampMicros);
      g.writeNumberField("elapsed", span.elapsedMicros());
      g.writeStringField("action", action);
      g.writeStringField("operation", span.operationName());
      g.writeStringField("traceId", context.traceId()
                                           .toString());
      g.writeStringField("spanId", context.spanId()
                                          .toString());

      Set<Entry<String, Object>> tagsEntrySet = span.tags()
                                                    .entrySet();
      if (!tagsEntrySet.isEmpty()) {
        g.writeObjectFieldStart("tags");
        for (Map.Entry<String, Object> kv : tagsEntrySet) {
          Object v = kv.getValue();
          if (v instanceof String) {
            g.writeStringField(kv.getKey(), (String) v);
          } else if (v instanceof Number) {
            g.writeNumberField(kv.getKey(), ((Number) v).doubleValue());
          } else if (v instanceof Boolean) {
            g.writeBooleanField(kv.getKey(), (Boolean) v);
          }
        }
        g.writeEndObject();
      }
      if (fields != null && !fields.isEmpty()) {
        g.writeObjectFieldStart("fields");
        for (Map.Entry<String, ?> kv : fields.entrySet()) {
          Object v = kv.getValue();
          if (v instanceof String) {
            g.writeStringField(kv.getKey(), (String) v);
          } else if (v instanceof Number) {
            g.writeNumberField(kv.getKey(), ((Number) v).doubleValue());
          } else if (v instanceof Boolean) {
            g.writeBooleanField(kv.getKey(), (Boolean) v);
          } else {
            g.writeStringField(kv.getKey(), String.valueOf(v));
          }
        }
        g.writeEndObject();
      } else {
        boolean baggageStart = false;
        for (Map.Entry<String, String> kv : span.context()
                                                .baggageItems()) {
          if (!baggageStart) {
            g.writeObjectFieldStart("baggage");
            baggageStart = true;
          }
          g.writeStringField(kv.getKey(), kv.getValue());
        }
        if (baggageStart) {
          g.writeEndObject();
        }
        List<Reference> references = span.references();
        if (!references.isEmpty()) {
          g.writeObjectFieldStart("references");
          for (Reference reference : references) {
            g.writeStringField(reference.getReferenceType(), reference.getContext()
                                                                      .spanId()
                                                                      .toString());
          }
          g.writeEndObject();
        }
      }

      g.writeEndObject();
      g.close();
      w.close();
      return w.toString();
    } catch (Exception exc) {
      exc.printStackTrace();
    }
    return "";
  }
}
