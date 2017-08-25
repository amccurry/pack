package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

public class Ext4LinuxFileSystem extends BaseLinuxFileSystem {

  public static final Ext4LinuxFileSystem INSTANCE = new Ext4LinuxFileSystem();

  private static final String BLOCK_SIZE_SWITCH = "-b";
  private static final String FORCE_CHECKING_SWITCH = "-f";
  private static final String FORCE_SWITCH = "-F";
  private static final String RESIZE2FS = "resize2fs";
  private static final String E2FSCK = "e2fsck";
  private static final String MKFS_EXT4 = "mkfs.ext4";

  @Override
  public void mkfs(File device, int blockSize) throws IOException {
    exec(MKFS_EXT4, BLOCK_SIZE_SWITCH, Integer.toString(blockSize), FORCE_SWITCH, device.getAbsolutePath());
  }

  @Override
  public void growOffline(File device) throws IOException {
    exec(E2FSCK, FORCE_CHECKING_SWITCH, device.getAbsolutePath());
    exec(RESIZE2FS, device.getAbsolutePath());
  }

  @Override
  public boolean isGrowOfflineSupported() {
    return true;
  }

}
