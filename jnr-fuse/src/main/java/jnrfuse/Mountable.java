package jnrfuse;

import java.nio.file.Path;

/**
 * @author Sergey Tselovalnikov
 * @since 03.06.15
 */
public interface Mountable {
  /**
   * Mount is not safe to invoke multiple times in default implementation
   *
   * Add option -h to see list of all available options
   */
  void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts);

  void umount();

  default void mount(Path mountPoint, boolean blocking, boolean debug) {
    mount(mountPoint, blocking, debug, new String[] { "-o", "allow_root", "-o", "auto_unmount" });
  }

  default void mount(Path mountPoint, boolean blocking) {
    mount(mountPoint, blocking, false);
  }

  default void mount(Path mountPoint) {
    mount(mountPoint, false);
  }
}
