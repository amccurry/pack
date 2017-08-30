package pack.block.server.fs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackServer;
import pack.PackServer.Result;

public abstract class BaseLinuxFileSystem implements LinuxFileSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseLinuxFileSystem.class);

  private static final String VERBOSE_SWITCH = "--verbose";
  private static final String UMOUNT = "umount";
  private static final String MOUNT = "mount";
  private static final String OPTIONS_SWITCH = "-o";
  private static final String DEFAULT_MOUNT_OPTIONS = "noatime,sync";

  @Override
  public void mount(File device, File mountLocation) throws IOException {
    exec(MOUNT, VERBOSE_SWITCH, OPTIONS_SWITCH, DEFAULT_MOUNT_OPTIONS, device.getAbsolutePath(),
        mountLocation.getAbsolutePath());
  }

  @Override
  public void umount(File mountLocation) throws IOException {
    exec(UMOUNT, mountLocation.getAbsolutePath());
  }

  protected void exec(String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    LOGGER.info("Executing command id {} cmd {}", uuid, list);
    Result result;
    try {
      result = PackServer.exec(uuid, list, LOGGER);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      LOGGER.info("Command id {} complete", uuid);
    }
    if (result.exitCode != 0) {
      throw new IOException("Unknown error while trying to run command " + Arrays.asList(command));
    }
  }

}
