package pack.block.server.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackServer.Result;
import pack.block.util.Utils;

public abstract class BaseLinuxFileSystem implements LinuxFileSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseLinuxFileSystem.class);

  protected static final String SUDO = "sudo";
  protected static final String TYPE = "TYPE";
  protected static final String TAG_SWITCH = "-s";
  protected static final String VALUE = "value";
  protected static final String OUTPUT_FORMAT_SWITCH = "-o";
  protected static final String BLKID = "blkid";
  protected static final String VERBOSE_SWITCH = "--verbose";
  protected static final String UMOUNT = "umount";
  protected static final String MOUNT = "mount";
  protected static final String OPTIONS_SWITCH = "-o";
  protected static final String DEFAULT_MOUNT_OPTIONS = "noatime,sync";

  @Override
  public void mount(File device, File mountLocation, String options) throws IOException {
    try {
      umount(mountLocation);
      LOGGER.info("Old mount was not cleanly umounted {}", mountLocation);
    } catch (IOException e) {

    }
    options = options == null ? DEFAULT_MOUNT_OPTIONS : options;
    Utils.exec(LOGGER, SUDO, MOUNT, VERBOSE_SWITCH, OPTIONS_SWITCH, options, device.getAbsolutePath(),
        mountLocation.getAbsolutePath());
  }

  @Override
  public void umount(File mountLocation) throws IOException {
    while (isMounted(mountLocation)) {
      LOGGER.info("Trying to umount {}", mountLocation);
      if (Utils.execReturnExitCode(LOGGER, SUDO, UMOUNT, mountLocation.getAbsolutePath()) == 0) {
        return;
      }
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

  }

  @Override
  public boolean isMounted(File mountLocation) throws IOException {
    Result result = Utils.execAsResultQuietly(LOGGER, SUDO, MOUNT);
    BufferedReader reader = new BufferedReader(new StringReader(result.stdout));
    String line;
    String path = mountLocation.getAbsolutePath()
                               .replace("/./", "/");
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      LOGGER.info("mount check {} {}", path, line);
      if (line.contains(path)) {
        LOGGER.info("mount found {} {}", path, line);
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isFileSystemExists(File device) throws IOException {
    if (Utils.execReturnExitCode(LOGGER, SUDO, BLKID, OUTPUT_FORMAT_SWITCH, VALUE, TAG_SWITCH, TYPE,
        device.getAbsolutePath()) == 0) {
      return true;
    }
    return false;
  }

}
