package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.Utils;

public class Ext4LinuxFileSystem extends BaseLinuxFileSystem {

  public static final Ext4LinuxFileSystem INSTANCE = new Ext4LinuxFileSystem();

  private static final Logger LOGGER = LoggerFactory.getLogger(Ext4LinuxFileSystem.class);

  private static final String EXT4 = "EXT4";
  private static final String RESIZE2FS = "resize2fs";
  private static final String FORCE_SWITCH = "-F";
  private static final String MKFS_EXT4 = "mkfs.ext4";

  @Override
  public void mkfs(File device, int blockSize) throws IOException {
    Utils.exec(LOGGER, MKFS_EXT4, FORCE_SWITCH, device.getAbsolutePath());
  }

  @Override
  public void growOffline(File device) throws IOException {
    Utils.exec(LOGGER, RESIZE2FS, FORCE_SWITCH, device.getAbsolutePath());
  }

  @Override
  public boolean isGrowOfflineSupported() {
    return false;
  }

  @Override
  public String getType() {
    return EXT4;
  }

}
