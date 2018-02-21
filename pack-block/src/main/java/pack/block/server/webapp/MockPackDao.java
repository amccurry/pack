package pack.block.server.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import pack.block.blockstore.hdfs.HdfsMetaData;

public class MockPackDao implements PackDao {

  private final Map<String, HdfsMetaData> _meta = new ConcurrentHashMap<>();

  @Override
  public List<String> listVolumes(String filter) throws IOException {
    return new ArrayList<>(_meta.keySet());
  }

  @Override
  public void delete(String name) throws IOException {
    _meta.remove(name);
  }

  @Override
  public HdfsMetaData get(String name) throws IOException {
    return _meta.get(name);
  }

  @Override
  public void createOrUpdate(String name, HdfsMetaData options) {
    _meta.put(name, options);
  }

}
