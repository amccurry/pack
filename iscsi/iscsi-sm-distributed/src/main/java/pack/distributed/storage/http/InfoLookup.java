package pack.distributed.storage.http;

import java.util.List;

public interface InfoLookup {

  List<TargetServerInfo> getTargets();

  List<Volume> getVolumes();
  
  List<CompactorServerInfo> getCompactors();

}
