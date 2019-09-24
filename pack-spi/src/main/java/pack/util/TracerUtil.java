package pack.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

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

  public static Scope trace(Class<?> clazz, String spanName) {
    Tracer tracer = Tracing.getTracer();
    Scope scope = tracer.spanBuilder(clazz.getName() + " " + spanName)
                        .startScopedSpan();
    List<GCData> gcDataListBefore = getGCDataList();
    return new Scope() {
      @Override
      public void close() {
        Span span = tracer.getCurrentSpan();
        addGcInfoIfNeeded(span, gcDataListBefore);
        scope.close();
      }
    };
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
      Map<String, AttributeValue> attributes = new HashMap<>();
      long id = nextGCData.getId();
      long start = nextGCData.getStart();
      long end = nextGCData.getEnd();
      attributes.put(TIME, AttributeValue.longAttributeValue(end - start));
      attributes.put(ID, AttributeValue.longAttributeValue(id));
      span.addAnnotation(GC_EVENT, attributes);
    }
  }
}
