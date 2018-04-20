package pack.block.blockstore.hdfs.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

public class LastestHdfsSnapshotStrategy extends HdfsSnapshotStrategy {

  private static final String MAX_NUMBER_OF_MOUNT_SNAPSHOTS_DEFAULT = "5";
  private static final String MAX_NUMBER_OF_MOUNT_SNAPSHOTS = "max.number.of.mount.snapshots";
  private final int _maxNumberOfMountSnapshots;

  public LastestHdfsSnapshotStrategy() {
    _maxNumberOfMountSnapshots = Integer.parseInt(
        System.getProperty(MAX_NUMBER_OF_MOUNT_SNAPSHOTS, MAX_NUMBER_OF_MOUNT_SNAPSHOTS_DEFAULT));
  }

  public static void setMaxNumberOfMountSnapshots(int maxNumberOfMountSnapshots) {
    System.setProperty(MAX_NUMBER_OF_MOUNT_SNAPSHOTS, Integer.toString(maxNumberOfMountSnapshots));
  }

  @Override
  public Collection<String> getSnapshotsToRemove(Collection<String> currentSnapshots) {
    List<String> list = new ArrayList<>(currentSnapshots);
    Collections.sort(list);
    Collections.reverse(list);
    int size = list.size();
    return ImmutableList.copyOf(list.subList(Math.min(_maxNumberOfMountSnapshots, size), size));
  }

}
