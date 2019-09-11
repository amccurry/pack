package pack.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Notification;
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

@SuppressWarnings("restriction")
public class TracerUtil {

  static {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    long startTime = runtimeMXBean.getStartTime();

    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean bean : garbageCollectorMXBeans) {

      NotificationEmitter emitter = (NotificationEmitter) bean;

      NotificationListener listener = new NotificationListener() {

        @Override
        public void handleNotification(Notification notification, Object handback) {
          if (notification.getType()
                          .equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from(
                (CompositeData) notification.getUserData());
            
            GcInfo gcInfo = info.getGcInfo();
            long id = gcInfo.getId();
            long gcStartTime = gcInfo.getStartTime();
            long gcEndTime = gcInfo.getEndTime();
            System.out.println(startTime + " " + System.currentTimeMillis() + " "
                + (System.currentTimeMillis() - startTime) + " " + gcStartTime + " " + gcEndTime);
          }
        }
      };
      emitter.addNotificationListener(listener, null, null);

    }
  }

  public static Scope trace(String spanName) {
    Tracer tracer = Tracing.getTracer();
    Scope scope = tracer.spanBuilder(spanName)
                        .startScopedSpan();
    return new Scope() {
      @Override
      public void close() {
        Span span = tracer.getCurrentSpan();
        addGcInfoIfNeeded(span);
        scope.close();
      }
    };
  }

  private static void addGcInfoIfNeeded(Span span) {
    Map<String, AttributeValue> attributes = new HashMap<>();
    attributes.put("time", AttributeValue.longAttributeValue(12345));
    span.addAnnotation("gc event", attributes);
  }
}
