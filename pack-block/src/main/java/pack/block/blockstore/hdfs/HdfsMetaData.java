package pack.block.blockstore.hdfs;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import pack.block.server.fs.FileSystemType;

@Getter
@FieldDefaults(makeFinal = false, level = AccessLevel.PRIVATE)
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
public class HdfsMetaData {

  private static final int DEFAULT_MAX_COMMITS_PER_ACTIVE_FILE = 16;

  public static final int DEFAULT_FILESYSTEM_BLOCKSIZE = 4096;

  // 100GB
  public static final long DEFAULT_LENGTH_BYTES = (long) (100L * Math.pow(1024, 3));

  // 1GB
  public static final long DEFAULT_MAX_BLOCK_FILE_SIZE = (long) (1L * Math.pow(1024, 3));

  public static final double DEFAULT_MAX_OBSOLETE_RATIO = 0.5;

  public static final HdfsMetaData DEFAULT_META_DATA = HdfsMetaData.builder()
                                                                   .fileSystemBlockSize(DEFAULT_FILESYSTEM_BLOCKSIZE)
                                                                   .fileSystemType(FileSystemType.XFS)
                                                                   .length(DEFAULT_LENGTH_BYTES)
                                                                   .maxBlockFileSize(DEFAULT_MAX_BLOCK_FILE_SIZE)
                                                                   .maxObsoleteRatio(DEFAULT_MAX_OBSOLETE_RATIO)
                                                                   .maxCommitsPerActiveFile(
                                                                       DEFAULT_MAX_COMMITS_PER_ACTIVE_FILE)
                                                                   .build();

  @JsonProperty
  long length = DEFAULT_LENGTH_BYTES;

  @JsonProperty
  FileSystemType fileSystemType = FileSystemType.XFS;

  @JsonProperty
  int fileSystemBlockSize = DEFAULT_FILESYSTEM_BLOCKSIZE;

  @JsonProperty
  long maxBlockFileSize = DEFAULT_MAX_BLOCK_FILE_SIZE;

  @JsonProperty
  String mountOptions;

  @JsonProperty
  double maxObsoleteRatio = DEFAULT_MAX_OBSOLETE_RATIO;

  @JsonProperty
  int maxCommitsPerActiveFile = DEFAULT_MAX_COMMITS_PER_ACTIVE_FILE;

  public static void main(String[] args) {
    System.out.println(DEFAULT_META_DATA);
  }

}
