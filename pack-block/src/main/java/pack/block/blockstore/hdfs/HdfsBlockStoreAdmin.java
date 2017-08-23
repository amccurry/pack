package pack.block.blockstore.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HdfsBlockStoreAdmin {

  public static final String METADATA = ".metadata";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) {

  }

  public static void writeHdfsMetaData(HdfsMetaData metaData, FileSystem fileSystem, Path volumePath)
      throws IOException {
    try (OutputStream output = fileSystem.create(new Path(volumePath, METADATA))) {
      MAPPER.writeValue(output, metaData);
    }
  }

  public static HdfsMetaData readMetaData(FileSystem fileSystem, Path volumePath) throws IOException {
    try (InputStream input = fileSystem.open(new Path(volumePath, METADATA))) {
      return MAPPER.readValue(input, HdfsMetaData.class);
    }
  }
}
