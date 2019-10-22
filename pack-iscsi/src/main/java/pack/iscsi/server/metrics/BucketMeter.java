package pack.iscsi.server.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import pack.iscsi.spi.metric.Meter;

public class BucketMeter implements Meter {

  private final AtomicReference<Bucket> _currentBucket = new AtomicReference<>();
  private final AtomicReference<Bucket[]> _oldBuckets = new AtomicReference<>();
  private final int _maxBuckets;
  private final long _bucketSpanInMillis;
  private final TimeUnit _bucketSpanUnit;
  private final long _bucketSpan;

  @Value
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @AllArgsConstructor
  @Builder(toBuilder = true)
  public static class BucketEntry {
    long tsMillis;
    long count;
  }

  public BucketMeter(int maxBuckets, long bucketSpan, TimeUnit bucketSpanUnit) {
    _maxBuckets = maxBuckets;
    _oldBuckets.set(new Bucket[maxBuckets]);
    _bucketSpanInMillis = bucketSpanUnit.toMillis(bucketSpan);
    _bucketSpanUnit = bucketSpanUnit;
    _bucketSpan = bucketSpan;
  }

  public int getMaxBuckets() {
    return _maxBuckets;
  }

  public TimeUnit getBucketSpanUnit() {
    return _bucketSpanUnit;
  }

  public long getBucketSpan() {
    return _bucketSpan;
  }

  public long getCurrentBucketCount() {
    Bucket bucket = _currentBucket.get();
    if (bucket != null) {
      return bucket._count.longValue();
    }
    return 0;
  }

  @Override
  public void mark(int count) {
    Bucket bucket = getBucket();
    bucket._count.add(count);
  }

  public BucketEntry[] getEntries() {
    BucketEntry[] values = new BucketEntry[_maxBuckets];
    Bucket[] buckets = _oldBuckets.get();
    for (int i = 0; i < _maxBuckets; i++) {
      Bucket bucket = buckets[i];
      if (bucket != null) {
        values[i] = BucketEntry.builder()
                               .count(bucket._count.longValue())
                               .tsMillis(bucket._bucketId * _bucketSpanInMillis)
                               .build();
      }
    }
    return values;
  }

  private Bucket getBucket() {
    long now = System.currentTimeMillis();
    long bucketId = getBucketValue(now);
    Bucket bucket;
    while (true) {
      bucket = _currentBucket.get();
      if (bucket == null || bucket.getId() != bucketId) {
        Bucket newBucket = new Bucket(bucketId);
        if (_currentBucket.compareAndSet(bucket, newBucket)) {
          addOldBucket(bucket);
          return newBucket;
        }
      } else {
        return bucket;
      }
    }
  }

  private void addOldBucket(Bucket bucket) {
    Bucket[] buckets = _oldBuckets.get();
    Bucket[] newBuckets = new Bucket[_maxBuckets];
    System.arraycopy(buckets, 0, newBuckets, 1, buckets.length - 1);
    newBuckets[0] = bucket;
    _oldBuckets.set(newBuckets);
  }

  private long getBucketValue(long now) {
    return now / _bucketSpanInMillis;
  }

  private static class Bucket {
    private final LongAdder _count = new LongAdder();
    private final long _bucketId;

    Bucket(long bucketId) {
      _bucketId = bucketId;
    }

    long getId() {
      return _bucketId;
    }

  }

}
