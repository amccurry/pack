package pack.s3;

import java.util.List;

public interface MetadataStore {

  List<String> getVolumes() throws Exception;

  long getVolumeSize(String volumeName) throws Exception;

  void setVolumeSize(String volumeName, long length) throws Exception;

  void umount(String volumeName);

  void mount(String volumeName);

  boolean isMounted(String volumeName);

}