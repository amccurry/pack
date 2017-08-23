package pack.block.blockstore.hdfs;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import pack.block.server.fs.FileSystemType;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class HdfsMetaData {

  public static final int DEFAULT_FILESYSTEM_BLOCKSIZE = 4096;

  // 100GB
  public static final long DEFAULT_LENGTH_BYTES = (long) (100L * Math.pow(1024, 3));

  public static final HdfsMetaData DEFAULT_META_DATA = HdfsMetaData.builder()
                                                                   .fileSystemBlockSize(DEFAULT_FILESYSTEM_BLOCKSIZE)
                                                                   .fileSystemType(FileSystemType.XFS)
                                                                   .length(DEFAULT_LENGTH_BYTES)
                                                                   .build();

  @JsonProperty
  long length;

  @JsonProperty
  FileSystemType fileSystemType;

  @JsonProperty
  int fileSystemBlockSize;

}
