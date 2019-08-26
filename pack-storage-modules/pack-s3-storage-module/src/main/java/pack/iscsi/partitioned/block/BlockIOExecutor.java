package pack.iscsi.partitioned.block;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

public interface BlockIOExecutor {

  BlockIOResult exec(FileChannel channel, File file, int blockSize, long onDiskGeneration, BlockState onDiskState, long lastStoredGeneration) throws IOException;

}
