package pack.block.server.webapp;

import java.io.IOException;
import java.util.List;

import pack.block.blockstore.hdfs.HdfsMetaData;

public interface PackDao {

  List<String> listVolumes(String filter) throws IOException;

  void delete(String name) throws IOException;

  HdfsMetaData get(String name) throws IOException;

  void createOrUpdate(String name, HdfsMetaData options);

}
