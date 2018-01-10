package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.Utils;

public class XfsLinuxFileSystem extends BaseLinuxFileSystem {

  public static final XfsLinuxFileSystem INSTANCE = new XfsLinuxFileSystem();

  private static final Logger LOGGER = LoggerFactory.getLogger(XfsLinuxFileSystem.class);

  private static final String XFS = "XFS";
  private static final String GROWFS_SWITCH = "-d";
  private static final String XFS_GROWFS = "xfs_growfs";
  private static final String MKFS_XFS = "mkfs.xfs";
  private static final String FSTRIM = "fstrim";
  private static final String VERBOSE_SWITCH = "-v";

  @Override
  public void mkfs(File device, int blockSize) throws IOException {
    Utils.exec(LOGGER, SUDO, MKFS_XFS, device.getAbsolutePath());
  }

  @Override
  public void growOnline(File device, File mountLocation) throws IOException {
    Utils.exec(LOGGER, SUDO, XFS_GROWFS, GROWFS_SWITCH, mountLocation.getAbsolutePath());
  }

  @Override
  public boolean isGrowOnlineSupported() {
    return true;
  }

  @Override
  public String getType() {
    return XFS;
  }

  @Override
  public boolean isFstrimSupported() {
    return true;
  }

  @Override
  public void fstrim(File mountLocation) throws IOException {
    Utils.exec(LOGGER, SUDO, FSTRIM, VERBOSE_SWITCH, mountLocation.getAbsolutePath());
  }

}
