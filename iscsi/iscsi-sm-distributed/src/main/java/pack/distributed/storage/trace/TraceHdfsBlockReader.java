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
      _hashesMap.put(path, value);
      LOGGER.info("created trace writer {}", path);
      return new TraceWriter(value, writer);
    }
    return writer;
  }

  public static Reader traceIfEnabled(Reader reader, Path path) {
    if (LOGGER.isTraceEnabled() || Boolean.getBoolean("pack.trace")) {
      Map<Integer, String> hashMap = _hashesMap.get(path);
      if (hashMap == null) {
        LOGGER.error("could not trace reader, no input {}", path);
        return reader;
      }
      LOGGER.info("created trace reader {}", path);
      return new TraceReader(hashMap, reader);
    }
    return reader;
  }

}
