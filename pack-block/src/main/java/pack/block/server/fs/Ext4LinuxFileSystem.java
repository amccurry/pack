package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

public class Ext4LinuxFileSystem extends BaseLinuxFileSystem {

  public static final Ext4LinuxFileSystem INSTANCE = new Ext4LinuxFileSystem();

  private static final String RESIZE2FS = "resize2fs";
  private static final String FORCE_SWITCH = "-F";
  private static final String MKFS_EXT4 = "mkfs.ext4";

  @Override
  public void mkfs(File device, int blockSize) throws IOException {
    exec(MKFS_EXT4, FORCE_SWITCH, device.getAbsolutePath());
  }

  @Override
  public void growOffline(File device) throws IOException {
    exec(RESIZE2FS, FORCE_SWITCH, device.getAbsolutePath());
  }

  @Override
  public boolean isGrowOfflineSupported() {
    return true;
  }

}
