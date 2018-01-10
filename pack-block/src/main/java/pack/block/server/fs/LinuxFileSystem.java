package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

public interface LinuxFileSystem {

  void mount(File device, File mountLocation, String options) throws IOException;

  void umount(File mountLocation) throws IOException;

  void mkfs(File device, int blockSize) throws IOException;
  
  boolean isMounted(File mountLocation) throws IOException;

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

  boolean isFileSystemExists(File device) throws IOException;

  String getType();

  default boolean isFstrimSupported() {
    return false;
  }

  default void fstrim(File mountLocation) throws IOException {

  }

}
