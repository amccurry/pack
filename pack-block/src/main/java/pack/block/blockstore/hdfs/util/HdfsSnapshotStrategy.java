package pack.block.blockstore.hdfs.util;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

public abstract class HdfsSnapshotStrategy {

  public static final String YYYYMMDDKKMMSS = "yyyyMMddkkmmssSSS";
  protected static final String MOUNT = ".mount.";

  public abstract Collection<String> getSnapshotsToRemove(Collection<String> currentSnapshots);

  public String getMountSnapshotName(Date date) {
    String now = new SimpleDateFormat(YYYYMMDDKKMMSS).format(date);
    return MOUNT + now;
  }

}
