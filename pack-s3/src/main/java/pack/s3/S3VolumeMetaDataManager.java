package pack.s3;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class S3VolumeMetaDataManager {

  private final File _root;

  public S3VolumeMetaDataManager(File root) {
    _root = root;
    _root.mkdirs();
  }

  public S3VolumeMetaData getMetaData(String volume) throws IOException {
    S3VolumeMetaData metaData = new S3VolumeMetaData();
    File file = new File(_root, volume);
    if (file.exists()) {
      try (DataInputStream input = new DataInputStream(new FileInputStream(file))) {
        metaData.readFields(input);
      }
    }
    return metaData;
  }

  public void setMetaData(String volume, S3VolumeMetaData metaData) throws IOException {
    File file = new File(_root, volume);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(file))) {
      metaData.write(output);
    }
  }

}
