package pack.distributed.storage.http;

import java.util.List;

public interface PackDao {

  List<TargetServerInfo> getTargets();

  List<Volume> getVolumes();

  List<CompactorServerInfo> getCompactors();

  List<Metric> getMetrics();
  
  List<Session> getSessions();

}
