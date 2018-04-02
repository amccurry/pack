package pack.distributed.storage.hdfs;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

public interface HdfsBlockGarbageCollector extends Closeable {

  void add(Path path) throws IOException, InterruptedException;

}