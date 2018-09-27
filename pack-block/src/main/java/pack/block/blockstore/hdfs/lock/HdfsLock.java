package pack.block.blockstore.hdfs.lock;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.block.util.PackSizeOf;
import pack.block.util.Utils;

public class HdfsLock implements PackLock {

  private static final String FILE_NOT_FOUND_EXCEPTION = "java.io.FileNotFoundException";
  private static final String ALREADY_BEING_CREATED_EXCEPTION = "org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException";
  private static final String RECOVERY_IN_PRPGRESS_EXCEPTION = "org.apache.hadoop.hdfs.protocol.RecoveryInProgressException";

  private final Logger LOG = LoggerFactory.getLogger(HdfsLock.class);

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    Path path = new Path("/tmp/hdfs-lock");

    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.delete(path, false);

    LockLostAction lockLostAction = () -> System.exit(1);
    try (HdfsLock hdfsLock1 = new HdfsLock(configuration, path, lockLostAction)) {
      if (hdfsLock1.tryToLock()) {
        System.out.println("success! " + hdfsLock1.isLockOwner());
      } else {
        System.out.println("failed!");
      }

      // crashMe();

      Thread.sleep(TimeUnit.SECONDS.toMillis(10));

      try (HdfsLock hdfsLock = new HdfsLock(configuration, path, lockLostAction)) {
        if (!hdfsLock.tryToLock()) {
          System.out.println("success! " + hdfsLock.isLockOwner());
        } else {
          System.out.println("failed!");
        }
      }
    }

    try (HdfsLock hdfsLock = new HdfsLock(configuration, path, lockLostAction)) {
      if (hdfsLock.tryToLock()) {
        System.out.println("success! " + hdfsLock.isLockOwner());
      } else {
        System.out.println("failed! " + hdfsLock.isLockOwner());
      }
    }

  }

  private final AtomicBoolean _closed = new AtomicBoolean(false);
  private final Configuration _configuration;
  private final Path _path;
  private final AtomicReference<FSDataOutputStream> _outputRef = new AtomicReference<FSDataOutputStream>();
  private final Timer _timer;
  private final long _period = TimeUnit.SECONDS.toMillis(20);
  private final Runnable _shutdownHook = () -> IOUtils.closeQuietly(HdfsLock.this);
  private final AtomicBoolean _initialized = new AtomicBoolean();
  private final AtomicLong _blockId = new AtomicLong();
  private final LockLostAction _lockLostAction;
  private final AtomicBoolean _lockLost = new AtomicBoolean();

  public HdfsLock(Configuration configuration, Path path, LockLostAction lockLostAction)
      throws IOException, InterruptedException {
    _lockLostAction = lockLostAction;
    _configuration = configuration;
    _path = path;
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      FileSystem fileSystem = _path.getFileSystem(configuration);
      fileSystem.mkdirs(_path.getParent());
      return null;
    });
    _timer = new Timer("HdfsLock:" + _path.toString(), true);
    _timer.schedule(getTimerTask(), _period, _period);
    ShutdownHookManager.get()
                       .addShutdownHook(_shutdownHook, 1000);
  }

  private TimerTask getTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        try {
          checkAndKeepAlive();
        } catch (Exception e) {
          LOG.error("Unknown error during keep alive", e);
        }
      }
    };
  }

  private void checkAndKeepAlive() throws IOException {
    if (!_initialized.get()) {
      return;
    }
    if (isLockOwner()) {
      LOG.debug("keep alive {}", _path);
      writeDataForKeepAlive();
    } else {
      _lockLost.set(true);
      LOG.error("lost lock for {}", _path);
      _lockLostAction.lost();
    }
  }

  @Override
  public boolean isLockOwner() throws IOException {
    if (!_initialized.get()) {
      return false;
    }
    try {
      return _blockId.get() == getFirstBlockIdOfLockFile();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  private boolean tryToLockInternal() throws IOException {
    FileSystem fileSystem = _path.getFileSystem(_configuration);
    LOOP: while (true) {
      if (fileSystem.exists(_path)) {
        try {
          _outputRef.set(fileSystem.append(_path));
        } catch (RemoteException e) {
          switch (e.getClassName()) {
          case ALREADY_BEING_CREATED_EXCEPTION: {
            LOG.info("Path {} already locked", _path);
            return false;
          }
          case RECOVERY_IN_PRPGRESS_EXCEPTION: {
            LOG.info("Path {} already locked", _path);
            return false;
          }
          case FILE_NOT_FOUND_EXCEPTION: {
            // This happens when a file was detected and deleted before this process could try to append. 
            continue LOOP;
          }
          default:
            throw e;
          }
        }
      } else {
        _outputRef.set(fileSystem.create(_path, false));
      }
      writeDataForKeepAlive();
      _blockId.set(getFirstBlockIdOfLockFile());
      _initialized.set(true);
      return true;
    }
  }

  private long getFirstBlockIdOfLockFile() throws IOException {
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    try {
      return ugi.doAs((PrivilegedExceptionAction<Long>) () -> {
        DistributedFileSystem fileSystem = (DistributedFileSystem) _path.getFileSystem(_configuration);
        DFSClient client = fileSystem.getClient();
        LocatedBlocks locatedBlocks = client.getLocatedBlocks(_path.toUri()
                                                                   .getPath(),
            0, 1);
        List<LocatedBlock> list = locatedBlocks.getLocatedBlocks();
        if (list == null || list.isEmpty()) {
          return 0L;
        }
        LocatedBlock locatedBlock = list.get(0);
        ExtendedBlock block = locatedBlock.getBlock();
        return block.getBlockId();
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }

  private void writeDataForKeepAlive() throws IOException {
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        FSDataOutputStream outputStream = _outputRef.get();
        if (outputStream != null) {
          outputStream.write(0);
          outputStream.hsync();
        }
        return null;
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean tryToLock() throws IOException {
    UserGroupInformation ugi = Utils.getUserGroupInformation();
    try {
      return ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> tryToLockInternal());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!_closed.get()) {
      _closed.set(true);
      _timer.cancel();
      _timer.purge();
      UserGroupInformation ugi = Utils.getUserGroupInformation();
      try {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
          IOUtils.closeQuietly(_outputRef.getAndSet(null));
          if (isLockOwner()) {
            FileSystem fileSystem = _path.getFileSystem(_configuration);
            fileSystem.delete(_path, false);
          }
          return null;
        });
      } catch (InterruptedException e) {
        LOG.error("Unknown error", e);
      }
      ShutdownHookManager shutdownHookManager = ShutdownHookManager.get();
      if (!shutdownHookManager.isShutdownInProgress()) {
        shutdownHookManager.removeShutdownHook(_shutdownHook);
      }
    }
  }

  public static boolean isLocked(Configuration configuration, Path path) throws IOException, InterruptedException {
    try (HdfsLock lock = new HdfsLock(configuration, path, () -> {

    })) {
      return !lock.tryToLock();
    }
  }

  @Override
  public long getSizeOf() {
    return PackSizeOf.getSizeOfObject(this, true);
  }
}
