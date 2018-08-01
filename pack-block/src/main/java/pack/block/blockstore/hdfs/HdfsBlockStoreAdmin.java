package pack.block.blockstore.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import pack.block.blockstore.hdfs.file.BlockFile;

public class HdfsBlockStoreAdmin {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsBlockStoreAdmin.class);

  public static final String METADATA = ".metadata";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.addResource(new FileInputStream("tmp-conf/hdfs-site.xml"));

    FileSystem fileSystem = FileSystem.get(configuration);

    Path volumePath = new Path("/block/testing1");
    HdfsMetaData metaData = readMetaData(fileSystem, volumePath);
    System.out.println(metaData);

    HdfsMetaData newMetaData = metaData.toBuilder()
                                       .length(metaData.getLength() * 2)
                                       .build();

    writeHdfsMetaData(newMetaData, fileSystem, volumePath);

  }

  public static boolean createVolume(FileSystem fileSystem, CreateVolumeRequest request) throws IOException {
    Path volumePath = request.getVolumePath();
    String volumeName = request.getVolumeName();
    if (!fileSystem.exists(volumePath)) {
      if (fileSystem.mkdirs(volumePath)) {
        clonePath(fileSystem, request.getClonePath(), request.getVolumePath(), request.isSymlinkClone());
        LOGGER.info("Create volume {}", volumeName);
        HdfsMetaData metaData = request.getMetaData();
        LOGGER.info("HdfsMetaData volume {} {}", volumeName, metaData);
        HdfsBlockStoreAdmin.writeHdfsMetaData(metaData, fileSystem, volumePath);
        return true;
      } else {
        LOGGER.info("Create not created volume {}", volumeName);
      }
    }
    return false;
  }

  public static boolean hasMetaData(FileSystem fileSystem, Path volumePath) throws IOException {
    return fileSystem.exists(new Path(volumePath, METADATA));
  }

  public static void clonePath(FileSystem fileSystem, Path srcPath, Path volumePath, boolean symlinkClone)
      throws IOException {
    if (srcPath == null) {
      return;
    }
    Path srcBlocks = new Path(srcPath, HdfsBlockStoreConfig.BLOCK);
    Path volumeBlocks = new Path(volumePath, HdfsBlockStoreConfig.BLOCK);
    if (fileSystem.exists(srcBlocks)) {
      if (symlinkClone) {
        if (!BlockFile.createLinkDir(fileSystem, srcBlocks, volumeBlocks)) {
          throw new IOException("Could not symlinkClone " + srcBlocks + " to " + volumeBlocks);
        }
      } else {
        FileStatus[] listStatus = fileSystem.listStatus(srcBlocks);
        for (FileStatus fileStatus : listStatus) {
          Path src = fileStatus.getPath();
          Path dst = new Path(volumeBlocks, src.getName());
          try (FSDataInputStream inputStream = fileSystem.open(src)) {
            try (FSDataOutputStream outputStream = fileSystem.create(dst)) {
              IOUtils.copy(inputStream, outputStream);
            }
          }
        }
      }
    }
  }

  public static void writeHdfsMetaData(HdfsMetaData metaData, FileSystem fileSystem, Path volumePath)
      throws IOException {
    try (OutputStream output = fileSystem.create(new Path(volumePath, METADATA))) {
      MAPPER.writeValue(output, metaData);
    }
  }

  public static HdfsMetaData readMetaData(FileSystem fileSystem, Path volumePath) throws IOException {
    if (!fileSystem.exists(new Path(volumePath, METADATA))) {
      return null;
    }
    HdfsMetaData hdfsMetaData;
    try (InputStream input = fileSystem.open(new Path(volumePath, METADATA))) {
      hdfsMetaData = MAPPER.readValue(input, HdfsMetaData.class);
    }
    return assignUuidIfMissing(hdfsMetaData, fileSystem, volumePath);
  }

  public static HdfsMetaData assignUuidIfMissing(HdfsMetaData hdfsMetaData, FileSystem fileSystem, Path volumePath)
      throws IOException {
    String uuid = hdfsMetaData.getUuid();
    if (uuid == null) {
      uuid = UUID.randomUUID()
                 .toString();
      HdfsMetaData newMetaData = hdfsMetaData.toBuilder()
                                             .uuid(uuid)
                                             .build();
      writeHdfsMetaData(newMetaData, fileSystem, volumePath);
      return newMetaData;
    }
    return hdfsMetaData;
  }
}
