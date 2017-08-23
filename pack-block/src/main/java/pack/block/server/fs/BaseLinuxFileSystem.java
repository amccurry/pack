package pack.block.server.fs;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.PackServer;
import pack.PackServer.Result;

public abstract class BaseLinuxFileSystem implements LinuxFileSystem {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseLinuxFileSystem.class);

  private static final String UMOUNT = "umount";
  private static final String MOUNT = "mount";
  private static final String UTF_8 = "UTF-8";

  @Override
  public void mount(File device, File mountLocation) throws IOException {
    exec(MOUNT, device.getAbsolutePath(), mountLocation.getAbsolutePath());
  }

  @Override
  public void umount(File mountLocation) throws IOException {
    exec(UMOUNT, "-d", mountLocation.getAbsolutePath());
  }

  protected void exec(String... command) throws IOException {
    List<String> list = Arrays.asList(command);
    Result result;
    try {
      result = PackServer.exec(list);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    if (result.exitCode != 0) {
      LOGGER.info("{} STDOUT {}", list, IOUtils.toString(result.output, UTF_8));
      LOGGER.info("{} STDERR {}", list, IOUtils.toString(result.error, UTF_8));
      throw new IOException(IOUtils.toString(result.error, UTF_8));
    }
  }

}
