package pack.block.server.fs;

import java.io.File;
import java.io.IOException;

public class XfsLinuxFileSystem extends BaseLinuxFileSystem {

  public static final XfsLinuxFileSystem INSTANCE = new XfsLinuxFileSystem();

  private static final String BLOCK_SIZE_SWITCH = "-b";
  private static final String XFS_GROWFS = "xfs_growfs";
  private static final String MKFS_XFS = "mkfs.xfs";

  @Override
  public void mkfs(File device, int blockSize) throws IOException {
    exec(MKFS_XFS, BLOCK_SIZE_SWITCH, Integer.toString(blockSize), device.getAbsolutePath());
  }

  @Override
  public void growOnline(File device, File mountLocation) throws IOException {
    exec(XFS_GROWFS, "-d", mountLocation.getAbsolutePath());
  }

  @Override
  public boolean isGrowOnlineSupported() {
    return true;
  }

}
