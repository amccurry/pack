package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackServer.Result;
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
  private static final String DEVICE_IS_FILE = "-f";
  private static final String XFS_REPAIR = "xfs_repair";

  @Override
  public void mount(File device, File mountLocation, String options) throws IOException {
    try {
      umount(mountLocation);
      LOGGER.info("Old mount was not cleanly umounted {}", mountLocation);
    } catch (IOException e) {

    }
    options = options == null ? DEFAULT_MOUNT_OPTIONS : options;
    Result result = Utils.execAsResult(LOGGER, SUDO, MOUNT, VERBOSE_SWITCH, OPTIONS_SWITCH, options,
        device.getAbsolutePath(), mountLocation.getAbsolutePath());

    if (result.exitCode == 0) {
      return;
    }

    if (result.stderr.contains("Structure needs cleaning")) {
      Utils.exec(LOGGER, SUDO, XFS_REPAIR, DEVICE_IS_FILE, device.getAbsolutePath());
      Utils.exec(LOGGER, SUDO, MOUNT, VERBOSE_SWITCH, OPTIONS_SWITCH, options, device.getAbsolutePath(),
          mountLocation.getAbsolutePath());
    }
    throw new IOException(result.stderr);
  }

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
