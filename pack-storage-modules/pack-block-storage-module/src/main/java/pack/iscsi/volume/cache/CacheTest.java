package pack.iscsi.volume.cache;

import java.util.Random;
import java.util.UUID;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;

import pack.iscsi.util.Utils;
import pack.util.ExecutorUtil;

public class CacheTest {

  public static void main(String[] args) throws Exception {
    Random random = new Random();

    RemovalListener<String, byte[]> removalListener = new RemovalListener<String, byte[]>() {
      @Override
      public void onRemoval(@Nullable String key, byte @Nullable [] value, @NonNull RemovalCause cause) {

      }
    };

    Weigher<String, byte[]> weigher = (key, value) -> value.length;

    CacheLoader<String, byte[]> cacheLoader = key -> {
      synchronized (random) {
        return new byte[random.nextInt(100)];
      }
    };
    CacheLoader<String, byte[]> loader = cacheLoader;

    LoadingCache<String, byte[]> cache = Caffeine.newBuilder()
                                                 .executor(ExecutorUtil.getCallerRunExecutor())
                                                 .removalListener(removalListener)
                                                 .weigher(weigher)
                                                 .maximumWeight(1000)
                                                 .build(loader);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < 100; i++) {
          cache.put(UUID.randomUUID()
                        .toString(),
              new byte[10]);
        }
        while (true) {
          try {
            Thread.sleep(500);
            System.out.println(cache.estimatedSize() + " " + Utils.getMaximum(cache));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    });
    thread.setDaemon(true);
    thread.start();

    Thread.sleep(10000);
    Utils.setMaximum(cache, 500);
    cache.cleanUp();

    Thread.sleep(10000);
  }

}
