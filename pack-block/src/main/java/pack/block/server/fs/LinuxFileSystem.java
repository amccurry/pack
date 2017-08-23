package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

public interface LinuxFileSystem {

  void mount(File device, File mountLocation) throws IOException;

  void umount(File mountLocation) throws IOException;

  void mkfs(File device, int blockSize) throws IOException;

  default void growOffline(File device) throws IOException {
    throw new IOException("Not supported.");
  }

  default void growOnline(File device, File mountLocation) throws IOException {
    throw new IOException("Not supported.");
  }

  default boolean isGrowOnlineSupported() {
    return false;
  }

  default boolean isGrowOfflineSupported() {
    return false;
  }
}
