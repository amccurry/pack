package pack.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.contrib.reporter.Reporter;
import io.opentracing.contrib.reporter.TracerR;
import io.opentracing.contrib.reporter.slf4j.Slf4jReporter;
import io.opentracing.util.ThreadLocalScopeManager;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pack.util.tracer.PackTracer;

@SuppressWarnings("restriction")
public class TracerUtil {

  private static final String GC_EVENT = "gc event";
  private static final String ID = "id";
  private static final String TIME = "time";

  @Value
  @EqualsAndHashCode
  @Builder(toBuilder = true)
  static class GCData {
    String name;
    long id;
    long start;
    long end;
    AtomicReference<GCData> next = new AtomicReference<>();
  }

  private static final AtomicReferenceArray<GCData> GCINFO;
  public static final Tracer TRACER;

  static {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    long startTime = runtimeMXBean.getStartTime();
    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    GCINFO = new AtomicReferenceArray<>(garbageCollectorMXBeans.size());
    for (int i = 0; i < garbageCollectorMXBeans.size(); i++) {
      GCINFO.set(i, GCData.builder()
                          .build());
    }

    int i = 0;
    for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {
      int index = i;
      i++;
      NotificationEmitter emitter = (NotificationEmitter) bean;
      NotificationListener listener = (notification, handback) -> {
        if (notification.getType()
                        .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
          GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
              (CompositeData) notification.getUserData());
          GcInfo gcInfo = info.getGcInfo();

          GCData data = GCData.builder()
                              .id(gcInfo.getId())
                              .name(info.getGcName())
                              .start(getEpoche(startTime, gcInfo.getStartTime()))
                              .end(getEpoche(startTime, gcInfo.getEndTime()))
                              .build();

          GCData oldGCData = GCINFO.get(index);
          if (oldGCData != null) {
            oldGCData.getNext()
                     .set(data);
          }
          GCINFO.set(index, data);
        }
      };
      emitter.addNotificationListener(listener, null, null);
    }
  }

  private static long getEpoche(long startTime, long gcTime) {
    return startTime + gcTime;
  }

  public static <T> Supplier<T> traceSupplier(Class<?> clazz, String name, Supplier<T> supplier) {
    Span activeSpan = TRACER.activeSpan();
    return () -> {
      Span span = getSpan(clazz, name, activeSpan);
      try (Scope scope = trace(span, TRACER.activateSpan(span))) {
        return supplier.get();
      }
    };
  }

  public static <T> Callable<T> traceCallable(Class<?> clazz, String name, Callable<T> callable) {
    Span activeSpan = TRACER.activeSpan();
    return () -> {
      Span span = getSpan(clazz, name, activeSpan);
      try (Scope scope = trace(span, TRACER.activateSpan(span))) {
        return callable.call();
      }
    };
  }

  public static Scope trace(Class<?> clazz, String name) {
    Span activeSpan = TRACER.activeSpan();
    Span span = getSpan(clazz, name, activeSpan);
    Scope scope = TRACER.activateSpan(span);
    return trace(span, scope);
  }

  private static Scope trace(Span span, Scope scope) {
    List<GCData> gcDataListBefore = getGCDataList();
    return new Scope() {
      @Override
      public void close() {
        addGcInfoIfNeeded(span, gcDataListBefore);
        scope.close();
        span.finish();
      }
    };
  }

  private static Span getSpan(Class<?> clazz, String name, Span activeSpan) {
    String operationName = getSpanNameFromClass(clazz) + " " + name;
    Span span;
    if (activeSpan == null) {
      span = TRACER.buildSpan(operationName)
                   .start();
    } else {
      span = TRACER.buildSpan(operationName)
                   .asChildOf(activeSpan)
                   .start();
    }
    return span;
  }

  static {
    Logger logger = LoggerFactory.getLogger("TRACER");
    ThreadLocalScopeManager scopeManager = new ThreadLocalScopeManager();
    Tracer backend = new PackTracer(scopeManager);
    Reporter reporter = new Slf4jReporter(logger, true);
    TRACER = new TracerR(backend, reporter, scopeManager);
  }

  private static final ConcurrentMap<Class<?>, String> NAME_CACHE = new ConcurrentHashMap<>();

  private static String getSpanNameFromClass(Class<?> clazz) {
    String name = NAME_CACHE.get(clazz);
    if (name != null) {
      return name;
    }
    NAME_CACHE.put(clazz, name = calculateSpanName(clazz));
    return name;
  }

  private static String calculateSpanName(Class<?> clazz) {
    StringBuilder builder = new StringBuilder();
    String clazzName = clazz.getName();
    int index = 0;
    while (true) {
      int nextIndex = clazzName.indexOf('.', index);
      if (nextIndex < 0) {
        builder.append(clazzName.substring(index)
                                .replace('$', '.'));
        return builder.toString();
      } else {
        builder.append(clazzName.charAt(index));
        builder.append('.');
        index = nextIndex + 1;
      }
    }
  }

  private static List<GCData> getGCDataList() {
    List<GCData> gcDataList = new ArrayList<>();
    for (int i = 0; i < GCINFO.length(); i++) {
      gcDataList.add(GCINFO.get(i));
    }
    return gcDataList;
  }

  private static void addGcInfoIfNeeded(Span span, List<GCData> gcDataListBefore) {
    for (GCData data : gcDataListBefore) {
      if (data != null) {
        addGcInfoIfNeeded(span, data);
      }
    }
  }

  private static void addGcInfoIfNeeded(Span span, GCData data) {
    GCData nextGCData = data.getNext()
                            .get();
    if (nextGCData != null) {
      long id = nextGCData.getId();
      long start = nextGCData.getStart();
      long end = nextGCData.getEnd();
      span.log(GC_EVENT + " " + ID + " (" + id + ") " + TIME + "(" + (end - start) + ")");
    }
  }

  public static void traceLog(String event) {
    TRACER.activeSpan()
          .log(event);
  }

}
