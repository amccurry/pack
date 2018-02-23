package pack.block.blockstore.hdfs;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class HdfsMetaData {
  private static final String MAX_IDLE_WRITER_TIME = "maxIdleWriterTime";
  private static final String MIN_TIME_BETWEEN_SYNCS = "minTimeBetweenSyncs";
  private static final String FILE_SYSTEM_TYPE = "fileSystemType";
  private static final String MOUNT_OPTIONS = "mountOptions";
  private static final String MAX_BLOCK_FILE_SIZE = "maxBlockFileSize";
  private static final String LENGTH = "length";
  private static final String FILE_SYSTEM_BLOCK_SIZE = "fileSystemBlockSize";
  private static final String WAL_COMPRESSION_CODEC = "walCompressionCodec";
  private static final String WAL_COMPRESSION_TYPE = "walCompressionType";
  private static final String MAX_WAL_FILE_SIZE = "maxWalFileSize";
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

  public static final long DEFAULT_MAX_WAL_FILE_SIZE = 32 * 1024L * 1024L;

  public static final long DEFAULT_MAX_IDLE_WRITER_TIME = TimeUnit.SECONDS.toNanos(10);

  public static final long DEFAULT_MIN_TIME_BETWEEN_SYNCS = TimeUnit.MILLISECONDS.toMillis(10);

  public static final HdfsMetaData DEFAULT_META_DATA = HdfsMetaData.builder()
      .fileSystemBlockSize(DEFAULT_FILESYSTEM_BLOCKSIZE).fileSystemType(FileSystemType.XFS).length(DEFAULT_LENGTH_BYTES)
      .maxBlockFileSize(DEFAULT_MAX_BLOCK_FILE_SIZE).maxObsoleteRatio(DEFAULT_MAX_OBSOLETE_RATIO)
      .maxWalFileSize(DEFAULT_MAX_WAL_FILE_SIZE).maxIdleWriterTime(DEFAULT_MAX_IDLE_WRITER_TIME)
      .minTimeBetweenSyncs(DEFAULT_MIN_TIME_BETWEEN_SYNCS).build();

  @JsonProperty
  @Builder.Default
  long length = DEFAULT_LENGTH_BYTES;

  @JsonProperty
  @Builder.Default
  FileSystemType fileSystemType = FileSystemType.XFS;

  @JsonProperty
  @Builder.Default
  int fileSystemBlockSize = DEFAULT_FILESYSTEM_BLOCKSIZE;

  @JsonProperty
  @Builder.Default
  long maxBlockFileSize = DEFAULT_MAX_BLOCK_FILE_SIZE;

  @JsonProperty
  String mountOptions;

  @JsonProperty
  @Builder.Default
  double maxObsoleteRatio = DEFAULT_MAX_OBSOLETE_RATIO;

  @JsonProperty
  @Builder.Default
  long maxWalFileSize = DEFAULT_MAX_WAL_FILE_SIZE;

  @JsonProperty
  String walCompressionType;

  @JsonProperty
  String walCompressionCodec;

  @JsonProperty
  @Builder.Default
  long maxIdleWriterTime = DEFAULT_MAX_IDLE_WRITER_TIME;

  @JsonProperty
  @Builder.Default
  long minTimeBetweenSyncs = DEFAULT_MIN_TIME_BETWEEN_SYNCS;

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
    if (options.containsKey(MAX_WAL_FILE_SIZE)) {
      builder.maxWalFileSize(toLong(options.get(MAX_WAL_FILE_SIZE)));
    }
    if (options.containsKey(WAL_COMPRESSION_TYPE)) {
      builder.walCompressionType(toString(options.get(WAL_COMPRESSION_TYPE)));
    }
    if (options.containsKey(WAL_COMPRESSION_CODEC)) {
      builder.walCompressionCodec(toString(options.get(WAL_COMPRESSION_CODEC)));
    }
    if (options.containsKey(MAX_IDLE_WRITER_TIME)) {
      builder.maxIdleWriterTime(toLong(options.get(MAX_IDLE_WRITER_TIME)));
    }
    if (options.containsKey(MIN_TIME_BETWEEN_SYNCS)) {
      builder.minTimeBetweenSyncs(toLong(options.get(MIN_TIME_BETWEEN_SYNCS)));
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
