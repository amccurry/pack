package pack.s3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FileHandleManger {

  private final String _cacheLocation;
  private final long _blockSize = 64 * 1024 * 1024;
  private final S3VolumeMetaDataManager _metaDataManager;

  public FileHandleManger(String cacheLocation, String syncLocations) {
    _cacheLocation = cacheLocation;
    _metaDataManager = new S3VolumeMetaDataManager(new File(syncLocations));
  }

  public List<String> getVolumes() {
    return Arrays.asList("testv2");
  }

  public long getVolumeSize(String volumeName) {
    return 100L * 1024L * 1024L * 1024L;
  }

  public FileHandle createHandle(String volumeName) throws IOException {
    return new S3FileHandleV2("<your bucket here>", volumeName, _blockSize, _cacheLocation, _metaDataManager);
  }

}
