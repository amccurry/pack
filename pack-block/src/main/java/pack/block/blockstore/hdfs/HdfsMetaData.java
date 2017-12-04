package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

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
  private static final String MAX_COMMITS_PER_ACTIVE_FILE = "maxCommitsPerActiveFile";
  private static final String FILE_SYSTEM_TYPE = "fileSystemType";
  private static final String MOUNT_OPTIONS = "mountOptions";
  private static final String MAX_BLOCK_FILE_SIZE = "maxBlockFileSize";
  private static final String LENGTH = "length";
  private static final String FILE_SYSTEM_BLOCK_SIZE = "fileSystemBlockSize";
  private static final String WAL_COMPRESSION_CODEC = "walCompressionCodec";
  private static final String WAL_COMPRESSION_TYPE = "walCompressionType";
  private static final String MAX_WAL_FILE_SIZE = "maxWalFileSize";
  private static final String MAX_CACHE_CAP_PER_ACTIVE_FILE = "maxCacheCapPerActiveFile";
  private static final String MAX_CACHE_SIZE_PER_ACTIVE_FILE = "maxCacheSizePerActiveFile";
  private static final String MAX_OBSOLETE_RATIO = "maxObsoleteRatio";

  public static final int DEFAULT_MAX_CACHE_CAPACITY_PER_ACTIVE_FILE = 10_000;

  public static final long DEFAULT_MAX_CACHE_SIZE_PER_ACTIVE_FILE = 10_000_000L;

  public static final int DEFAULT_MAX_COMMITS_PER_ACTIVE_FILE = 128;

  public static final int DEFAULT_FILESYSTEM_BLOCKSIZE = 4096;

  // 100GB
  public static final long DEFAULT_LENGTH_BYTES = (long) (100L * Math.pow(1024, 3));

  // 1GB
  public static final long DEFAULT_MAX_BLOCK_FILE_SIZE = (long) (5L * Math.pow(1024, 3));

  public static final double DEFAULT_MAX_OBSOLETE_RATIO = 0.5;

  public static final long DEFAULT_MAX_WAL_FILE_SIZE = 256 * 1024L * 1024L;

  public static final long DEFAULT_MAX_IDLE_WRITER_TIME = TimeUnit.MINUTES.toNanos(10);

  public static final HdfsMetaData DEFAULT_META_DATA = HdfsMetaData.builder()
                                                                   .fileSystemBlockSize(DEFAULT_FILESYSTEM_BLOCKSIZE)
                                                                   .fileSystemType(FileSystemType.XFS)
                                                                   .length(DEFAULT_LENGTH_BYTES)
                                                                   .maxBlockFileSize(DEFAULT_MAX_BLOCK_FILE_SIZE)
                                                                   .maxObsoleteRatio(DEFAULT_MAX_OBSOLETE_RATIO)
                                                                   .maxCommitsPerActiveFile(
                                                                       DEFAULT_MAX_COMMITS_PER_ACTIVE_FILE)
                                                                   .maxCacheCapPerActiveFile(
                                                                       DEFAULT_MAX_CACHE_CAPACITY_PER_ACTIVE_FILE)
                                                                   .maxCacheSizePerActiveFile(
                                                                       DEFAULT_MAX_CACHE_SIZE_PER_ACTIVE_FILE)
                                                                   .maxWalFileSize(DEFAULT_MAX_WAL_FILE_SIZE)
                                                                   .maxIdleWriterTime(DEFAULT_MAX_IDLE_WRITER_TIME)
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

  @JsonProperty
  long maxCacheSizePerActiveFile = DEFAULT_MAX_CACHE_SIZE_PER_ACTIVE_FILE;

  @JsonProperty
  int maxCacheCapPerActiveFile = DEFAULT_MAX_CACHE_CAPACITY_PER_ACTIVE_FILE;

  @JsonProperty
  long maxWalFileSize = DEFAULT_MAX_WAL_FILE_SIZE;

  @JsonProperty
  String walCompressionType;

  @JsonProperty
  String walCompressionCodec;

  @JsonProperty
  long maxIdleWriterTime = DEFAULT_MAX_IDLE_WRITER_TIME;

  public static void main(String[] args) throws IOException {
    System.out.println(DEFAULT_META_DATA);
    ObjectMapper mapper = new ObjectMapper();
    System.out.println(mapper.writeValueAsString(DEFAULT_META_DATA));

    HdfsMetaData hdfsMetaData = mapper.readValue(new File("test.json"), HdfsMetaData.class);
    System.out.println(hdfsMetaData.getMaxIdleWriterTime());
  }

  public static HdfsMetaData setupOptions(HdfsMetaData defaultmetaData, Map<String, Object> options) {
    HdfsMetaDataBuilder builder = defaultmetaData.toBuilder();
    if (options.containsKey(LENGTH)) {
      builder.length(toLong(options.get(LENGTH)));
    }
    if (options.containsKey(FILE_SYSTEM_TYPE)) {
      builder.fileSystemType(toFileSystemType(options.get(FILE_SYSTEM_TYPE)));
    }
    if (options.containsKey(FILE_SYSTEM_BLOCK_SIZE)) {
      builder.fileSystemBlockSize(toInt(options.get(FILE_SYSTEM_BLOCK_SIZE)));
    }
    if (options.containsKey(MAX_BLOCK_FILE_SIZE)) {
      builder.maxBlockFileSize(toLong(options.get(MAX_BLOCK_FILE_SIZE)));
    }
    if (options.containsKey(MOUNT_OPTIONS)) {
      builder.mountOptions(toString(options.get(MOUNT_OPTIONS)));
    }
    if (options.containsKey(MAX_OBSOLETE_RATIO)) {
      builder.maxObsoleteRatio(toDouble(options.get(MAX_OBSOLETE_RATIO)));
    }
    if (options.containsKey(MAX_COMMITS_PER_ACTIVE_FILE)) {
      builder.maxCommitsPerActiveFile(toInt(options.get(MAX_COMMITS_PER_ACTIVE_FILE)));
    }
    if (options.containsKey(MAX_CACHE_SIZE_PER_ACTIVE_FILE)) {
      builder.maxCacheSizePerActiveFile(toLong(options.get(MAX_CACHE_SIZE_PER_ACTIVE_FILE)));
    }
    if (options.containsKey(MAX_CACHE_CAP_PER_ACTIVE_FILE)) {
      builder.maxCacheCapPerActiveFile(toInt(options.get(MAX_CACHE_CAP_PER_ACTIVE_FILE)));
    }
    if (options.containsKey(MAX_WAL_FILE_SIZE)) {
      builder.maxWalFileSize(toLong(options.get(MAX_WAL_FILE_SIZE)));
    }
    if (options.containsKey(WAL_COMPRESSION_TYPE)) {
      builder.walCompressionType(toString(options.get(WAL_COMPRESSION_TYPE)));
    }
    if (options.containsKey(WAL_COMPRESSION_CODEC)) {
      builder.walCompressionCodec(toString(options.get(WAL_COMPRESSION_CODEC)));
    }
    return builder.build();
  }

  public static double toDouble(Object object) {
    return Double.parseDouble(toString(object));
  }

  public static FileSystemType toFileSystemType(Object object) {
    return FileSystemType.valueOf(toString(object).toUpperCase());
  }

  public static String toString(Object object) {
    return object.toString();
  }

  public static long toLong(Object object) {
    return Long.parseLong(toString(object));
  }

  public static int toInt(Object object) {
    return Integer.parseInt(toString(object));
  }

}
