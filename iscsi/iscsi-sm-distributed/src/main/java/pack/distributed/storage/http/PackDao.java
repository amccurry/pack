package pack.distributed.storage.http;

import java.io.IOException;
import java.util.List;

import pack.distributed.storage.PackMetaData;

public interface PackDao {

  List<TargetServerInfo> getTargets();

  List<Volume> getVolumes();

  List<CompactorServerInfo> getCompactors();

  default void createVolume(String name, PackMetaData metaData) throws IOException {

  }

}
