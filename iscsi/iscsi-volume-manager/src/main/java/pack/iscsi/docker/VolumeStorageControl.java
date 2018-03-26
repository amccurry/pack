package pack.iscsi.docker;

import java.util.List;
import java.util.Map;

public interface VolumeStorageControl {

  void create(String volumeName, Map<String, Object> options) throws Exception;

  void remove(String volumeName) throws Exception;

  String mount(String volumeName, String id) throws Exception;

  String getMountPoint(String volumeName) throws Exception;

  void unmount(String volumeName, String id) throws Exception;

  List<String> listVolumes() throws Exception;

  boolean exists(String volumeName) throws Exception;

}
