package pack.distributed.storage.metrics.json;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

public class SetupJvmMetrics {

  private static final Timer TIMER = new Timer("jvm-metrics", true);
  private static boolean SETUP = false;

  public static synchronized void setup(MetricRegistry registry) {
    if (!SETUP) {
      TIMER.schedule(getTimerTask(registry), TimeUnit.SECONDS.toMillis(3), TimeUnit.SECONDS.toMillis(3));
      SETUP = true;
    }
  }

  private static TimerTask getTimerTask(MetricRegistry registry) {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    Histogram heapMemoryUsageCommittedHistogram = registry.histogram("jvm.heap.committed.histogram");
    AtomicLong heapMemoryUsageCommittedGauge = addLongGauge(registry, "jvm.heap.committed.gauge");

    Histogram heapMemoryUsageInitHistogram = registry.histogram("jvm.heap.init.histogram");
    AtomicLong heapMemoryUsageInitGauge = addLongGauge(registry, "jvm.heap.init.gauge");

    Histogram heapMemoryUsageMaxHistogram = registry.histogram("jvm.heap.max.histogram");
    AtomicLong heapMemoryUsageMaxGauge = addLongGauge(registry, "jvm.heap.max.gauge");

    Histogram heapMemoryUsageUsedHistogram = registry.histogram("jvm.heap.used.histogram");
    AtomicLong heapMemoryUsageUsedGauge = addLongGauge(registry, "jvm.heap.used.gauge");

    Histogram nonHeapMemoryUsageCommittedHistogram = registry.histogram("jvm.nonheap.committed.histogram");
    AtomicLong nonHeapMemoryUsageCommittedGauge = addLongGauge(registry, "jvm.nonheap.committed.gauge");

    Histogram nonHeapMemoryUsageInitHistogram = registry.histogram("jvm.nonheap.init.histogram");
    AtomicLong nonHeapMemoryUsageInitGauge = addLongGauge(registry, "jvm.nonheap.init.gauge");

    Histogram nonHeapMemoryUsageMaxHistogram = registry.histogram("jvm.nonheap.max.histogram");
    AtomicLong nonHeapMemoryUsageMaxGauge = addLongGauge(registry, "jvm.nonheap.max.gauge");

    Histogram nonHeapMemoryUsageUsedHistogram = registry.histogram("jvm.nonheap.used.histogram");
    AtomicLong nonHeapMemoryUsageUsedGauge = addLongGauge(registry, "jvm.nonheap.used.gauge");

    return new TimerTask() {
      @Override
      public void run() {
        {
          MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

          long committed = heapMemoryUsage.getCommitted();
          heapMemoryUsageCommittedHistogram.update(committed);
          heapMemoryUsageCommittedGauge.set(committed);

          long init = heapMemoryUsage.getInit();
          heapMemoryUsageInitHistogram.update(init);
          heapMemoryUsageInitGauge.set(init);

          long max = heapMemoryUsage.getMax();
          heapMemoryUsageMaxHistogram.update(max);
          heapMemoryUsageMaxGauge.set(max);

          long used = heapMemoryUsage.getUsed();
          heapMemoryUsageUsedHistogram.update(used);
          heapMemoryUsageUsedGauge.set(used);
        }
        {
          MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

          long committed = nonHeapMemoryUsage.getCommitted();
          nonHeapMemoryUsageCommittedHistogram.update(committed);
          nonHeapMemoryUsageCommittedGauge.set(committed);

          long init = nonHeapMemoryUsage.getInit();
          nonHeapMemoryUsageInitHistogram.update(init);
          nonHeapMemoryUsageInitGauge.set(init);

          long max = nonHeapMemoryUsage.getMax();
          nonHeapMemoryUsageMaxHistogram.update(max);
          nonHeapMemoryUsageMaxGauge.set(max);

          long used = nonHeapMemoryUsage.getUsed();
          nonHeapMemoryUsageUsedHistogram.update(used);
          nonHeapMemoryUsageUsedGauge.set(used);
        }

      }
    };
  }

  private static AtomicLong addLongGauge(MetricRegistry registry, String name) {
    AtomicLong atomicLong = new AtomicLong();
    registry.gauge(name, () -> () -> atomicLong.get());
    return atomicLong;
  }

}
