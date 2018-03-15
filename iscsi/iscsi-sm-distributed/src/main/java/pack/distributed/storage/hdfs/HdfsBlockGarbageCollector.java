package pack.distributed.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface HdfsBlockGarbageCollector {

  void add(Path path) throws IOException, InterruptedException;

}