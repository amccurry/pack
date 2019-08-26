package pack.iscsi.s3.v1;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface S3BlockIOExecutor {

  S3BlockState exec(FileChannel channel, long positionOfStartOfBlock, int blockSize) throws IOException;

}
