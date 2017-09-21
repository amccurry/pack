package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.Utils;

public abstract class BaseLinuxFileSystem implements LinuxFileSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseLinuxFileSystem.class);

  private static final String TYPE = "TYPE";
  private static final String TAG_SWITCH = "-s";
  private static final String VALUE = "value";
  private static final String OUTPUT_FORMAT_SWITCH = "-o";
  private static final String BLKID = "blkid";
  private static final String VERBOSE_SWITCH = "--verbose";
  private static final String UMOUNT = "umount";
  private static final String MOUNT = "mount";
  private static final String OPTIONS_SWITCH = "-o";
  // private static final String DEFAULT_MOUNT_OPTIONS = "noatime,sync";
  private static final String DEFAULT_MOUNT_OPTIONS = "noatime";

  @Override
  public void mount(File device, File mountLocation, String options) throws IOException {
    options = options == null ? DEFAULT_MOUNT_OPTIONS : options;
    Utils.exec(LOGGER, MOUNT, VERBOSE_SWITCH, OPTIONS_SWITCH, options, device.getAbsolutePath(),
        mountLocation.getAbsolutePath());
  }

  @Override
  public void umount(File mountLocation) throws IOException {
    Utils.exec(LOGGER, UMOUNT, mountLocation.getAbsolutePath());
  }

  @Override
  public boolean isFileSystemExists(File device) throws IOException {
    if (Utils.execReturnExitCode(LOGGER, BLKID, OUTPUT_FORMAT_SWITCH, VALUE, TAG_SWITCH, TYPE,
        device.getAbsolutePath()) == 0) {
      return true;
    }
    return false;
  }

}
