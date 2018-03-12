package pack.distributed.storage.trace;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.hdfs.BlockFile.Reader;
import pack.distributed.storage.hdfs.BlockFile.Writer;

public class TraceHdfsBlockReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TraceHdfsBlockReader.class);

  private static ConcurrentMap<Path, Map<Integer, String>> _hashesMap = new ConcurrentHashMap<>();

  public static Writer traceIfEnabled(Writer writer, Path path) {
    if (LOGGER.isTraceEnabled() || Boolean.getBoolean("pack.trace")) {
      Map<Integer, String> value = new ConcurrentHashMap<>();
      Map<Integer, String> hashMap = _hashesMap.putIfAbsent(path, value);
      if (hashMap == null) {
        hashMap = value;
      }
      LOGGER.info("created trace writer {}", path);
      return new TraceWriter(hashMap, writer);
    }
    return writer;
  }

  public static Reader traceIfEnabled(Reader reader, Path path) {
    if (LOGGER.isTraceEnabled() || Boolean.getBoolean("pack.trace")) {
      Map<Integer, String> hashMap = _hashesMap.get(path);
      LOGGER.info("created trace reader {}", path);
      return new TraceReader(hashMap, reader);
    }
    return reader;
  }

}
