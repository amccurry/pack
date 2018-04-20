package pack.block.blockstore.hdfs.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.Value;

public class TimeBasedHdfsSnapshotStrategy extends HdfsSnapshotStrategy {

  @Override
  public Collection<String> getSnapshotsToRemove(Collection<String> currentSnapshots) {
    long now = System.currentTimeMillis();

    List<Snapshot> snapshotList = new ArrayList<>();
    for (String s : currentSnapshots) {
      Snapshot snapshot = Snapshot.parse(s);
      if (snapshot != null) {
        snapshotList.add(snapshot);
      }
    }

    SnapshotBucket[] lastDayByHour = new SnapshotBucket[24];
    SnapshotBucket[] lastWeekByDay = new SnapshotBucket[7];
    SnapshotBucket[] last4WeeksByWeek = new SnapshotBucket[4];

    for (Snapshot snapshot : snapshotList) {
      if (snapshot.isDuringTheLastDay(now)) {
        int hour = (int) snapshot.getHour(now);
        if (lastDayByHour[hour] == null) {
          lastDayByHour[hour] = new SnapshotBucket();
        }
        lastDayByHour[hour].add(snapshot);
      }

      if (snapshot.isDuringTheLastWeek(now)) {
        int dayOfTheWeek = (int) snapshot.getDayOfTheWeek(now);
        if (lastWeekByDay[dayOfTheWeek] == null) {
          lastWeekByDay[dayOfTheWeek] = new SnapshotBucket();
        }
        lastWeekByDay[dayOfTheWeek].add(snapshot);
      }

      if (snapshot.isDuringTheLast4Weeks(now)) {
        int week = (int) snapshot.getWeekOfTheLast4Weeks(now);
        if (last4WeeksByWeek[week] == null) {
          last4WeeksByWeek[week] = new SnapshotBucket();
        }
        last4WeeksByWeek[week].add(snapshot);
      }
    }

    List<Snapshot> lastHourSnapshots = new ArrayList<>();
    List<Snapshot> lastWeekSnapshots = new ArrayList<>();
    List<Snapshot> last4WeekSnapshots = new ArrayList<>();

    for (SnapshotBucket bucket : lastDayByHour) {
      if (lastHourSnapshots.size() < 5) {
        Snapshot newest = getNewest(now, bucket);
        if (newest != null) {
          lastHourSnapshots.add(newest);
        }
      }
    }
    for (SnapshotBucket bucket : lastWeekByDay) {
      if (lastWeekSnapshots.size() < 7) {
        Snapshot newest = getNewest(now, bucket);
        if (newest != null) {
          lastWeekSnapshots.add(newest);
        }
      }
    }
    for (SnapshotBucket bucket : last4WeeksByWeek) {
      if (last4WeekSnapshots.size() < 4) {
        Snapshot newest = getNewest(now, bucket);
        if (newest != null) {
          last4WeekSnapshots.add(newest);
        }
      }
    }

    Set<Snapshot> snapshots = new HashSet<>();
    snapshots.addAll(lastHourSnapshots);
    snapshots.addAll(lastWeekSnapshots);
    snapshots.addAll(last4WeekSnapshots);

    List<String> results = new ArrayList<>(currentSnapshots);
    for (Snapshot snapshot : snapshots) {
      results.remove(snapshot.name);
    }
    return results;
  }

  private Snapshot getNewest(long now, SnapshotBucket bucket) {
    if (bucket == null || bucket.getSnapshots() == null || bucket.getSnapshots()
                                                                 .isEmpty()) {
      return null;
    }
    List<Snapshot> snapshots = new ArrayList<>(bucket.getSnapshots());
    Collections.sort(snapshots);
    return snapshots.get(0);
  }

  @Data
  @ToString
  public static class SnapshotBucket {
    List<Snapshot> snapshots = new ArrayList<>();

    public void add(Snapshot snapshot) {
      snapshots.add(snapshot);
    }
  }

  @Value
  @Builder
  @ToString
  public static class Snapshot implements Comparable<Snapshot> {
    String name;
    Date date;

    public static Snapshot parse(String name) {
      if (!name.startsWith(MOUNT)) {
        return null;
      }
      String dateStr = name.substring(MOUNT.length());
      Date date;
      try {
        date = new SimpleDateFormat(YYYYMMDDKKMMSS).parse(dateStr);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
      return Snapshot.builder()
                     .date(date)
                     .name(name)
                     .build();
    }

    public long getWeekOfTheLast4Weeks(long now) {
      return (now - date.getTime()) / TimeUnit.DAYS.toMillis(7);
    }

    public boolean isDuringTheLast4Weeks(long now) {
      return date.getTime() + TimeUnit.DAYS.toMillis(7 * 4) >= now;
    }

    public long getDayOfTheWeek(long now) {
      return (now - date.getTime()) / TimeUnit.DAYS.toMillis(1);
    }

    public boolean isDuringTheLastWeek(long now) {
      return date.getTime() + TimeUnit.DAYS.toMillis(7) >= now;
    }

    public long getHour(long now) {
      return (now - date.getTime()) / TimeUnit.HOURS.toMillis(1);
    }

    public boolean isDuringTheLastDay(long now) {
      return date.getTime() + TimeUnit.DAYS.toMillis(1) >= now;
    }

    @Override
    public int compareTo(Snapshot o) {
      return Long.compare(o.date.getTime(), date.getTime());
    }

  }

}
