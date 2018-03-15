package pack.distributed.storage.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.security.UserGroupInformation;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackHdfsBlockGarbageCollector implements HdfsBlockGarbageCollector, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PackHdfsBlockGarbageCollector.class);

  private final UserGroupInformation _ugi;
  private final Configuration _conf;
  private final Timer _timer;
  private final ConcurrentMap<Path, Long> _filesToBeRemoved = new ConcurrentHashMap<>();
  private final long _delayBeforeRemoval;

  public PackHdfsBlockGarbageCollector(UserGroupInformation ugi, Configuration conf, long delayBeforeRemoval) {
    _delayBeforeRemoval = delayBeforeRemoval;
    _ugi = ugi;
    _conf = conf;
    _timer = new Timer("hdfs-block-gc", true);
    _timer.schedule(getTask(), TimeUnit.MINUTES.toMillis(10), TimeUnit.MINUTES.toMillis(10));
  }

  private TimerTask getTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          _ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
              performGc();
              return null;
            }
          });
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
      }
    };
  }

  public void performGc() throws IOException {
    Trash trash = new Trash(_conf);
    for (Entry<Path, Long> e : new HashMap<>(_filesToBeRemoved).entrySet()) {
      Path path = e.getKey();
      Long ts = e.getValue();
      if (shouldBeDeleted(ts)) {
        trash.moveToTrash(path);
      }
    }
  }

  private boolean shouldBeDeleted(long ts) {
    return ts + _delayBeforeRemoval < System.currentTimeMillis();
  }

  @Override
  public void add(Path path) throws IOException, InterruptedException {
    Path qualPath = _ugi.doAs((PrivilegedExceptionAction<Path>) () -> {
      FileSystem fileSystem = path.getFileSystem(_conf);
      return fileSystem.makeQualified(path);
    });
    _filesToBeRemoved.putIfAbsent(qualPath, System.currentTimeMillis());
  }

  @Override
  public void close() throws IOException {
    _timer.purge();
    _timer.cancel();
  }

}
