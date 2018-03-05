package pack.hdfs.storage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

@Getter
@FieldDefaults(makeFinal = false, level = AccessLevel.PRIVATE)
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@Builder(toBuilder = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class HdfsMetaData {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // 100GB
  public static final long DEFAULT_LENGTH_BYTES = (long) (100L * Math.pow(1024, 3));

  @JsonProperty
  @Builder.Default
  long length = DEFAULT_LENGTH_BYTES;

  public static final int DEFAULT_BLOCK_SIZE = 4096;

  @JsonProperty
  @Builder.Default
  int blockSize = DEFAULT_BLOCK_SIZE;

  public static HdfsMetaData read(Configuration configuration, Path volume) throws IOException {
    Path path = getMeatDataPath(volume);
    FileSystem fileSystem = path.getFileSystem(configuration);
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.getName());
    }
    try (InputStream inputStream = fileSystem.open(path)) {
      return OBJECT_MAPPER.readValue(inputStream, HdfsMetaData.class);
    }
  }

  private static Path getMeatDataPath(Path volume) {
    return new Path(volume, ".metadata");
  }

  public void write(Configuration configuration, Path volume) throws IOException {
    Path path = getMeatDataPath(volume);
    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.mkdirs(volume);
    try (OutputStream outputStream = fileSystem.create(path, true)) {
      OBJECT_MAPPER.writeValue(outputStream, this);
    }
  }

}
