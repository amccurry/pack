package pack.block.blockstore.hdfs.util;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

public abstract class HdfsSnapshotStrategy {

  protected static final String MOUNT = ".mount.";
  protected static final String YYYYMMDDKKMMSS = "yyyyMMddkkmmssSSS";

  public abstract Collection<String> getSnapshotsToRemove(Collection<String> currentSnapshots);

  public String getMountSnapshotName(Date date) {
    String now = new SimpleDateFormat(YYYYMMDDKKMMSS).format(date);
    return MOUNT + now;
  }

}
