package pack.nativehdfs;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.FuseFileInfo;
import pack.block.fuse.FuseFileSystemSingleMount;

public class NativeFuse extends FuseStubFS implements Closeable {
  private static final String NONEMPTY = "nonempty";

  private static final Logger LOGGER = LoggerFactory.getLogger(FuseFileSystemSingleMount.class);

  public static final String BRICK = "brick";
  public static final String FUSE_PID = "fuse_pid";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";

  private final String _localPath;
  private final Path _path;
  private final Configuration _configuration;
  private final AtomicInteger _fileHandleCounter = new AtomicInteger(1);
  private final Set<Integer> _handles = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final Map<Integer, FSDataInputStream> _readers = new ConcurrentHashMap<>();
  private final Map<Integer, FSDataOutputStream> _writers = new ConcurrentHashMap<>();
  private final Map<String, Integer> _uids = new ConcurrentHashMap<>();
  private final Map<String, Integer> _gids = new ConcurrentHashMap<>();
  private final int _defaultGroupId;
  private final int _defaultOwnerId;

  public NativeFuse(String localPath, Configuration configuration, Path path) throws IOException {
    _localPath = localPath;
    _path = path;
    _configuration = configuration;
    _defaultGroupId = 0;
    _defaultOwnerId = 0;
  }

  private Path getPath(String path) {
    return new Path(_path, path);
  }

  public void localMount() {
    try {
      localMount(true);
    } catch (Throwable t) {
      LOGGER.error("Unknown error during fuse mount", t);
    }
  }

  public void localMount(boolean blocking) {
    jnr.ffi.Platform p = jnr.ffi.Platform.getNativePlatform();
    String[] opts;
    switch (p.getOS()) {
    case DARWIN:
      opts = new String[] { OPTION_SWITCH, ALLOW_ROOT };
      break;
    case LINUX:
    default:
      opts = new String[] { OPTION_SWITCH, ALLOW_ROOT, OPTION_SWITCH, AUTO_UNMOUNT, OPTION_SWITCH, SYNC, OPTION_SWITCH,
          NONEMPTY };
      break;
    }
    mount(Paths.get(_localPath), blocking, true, opts);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close");
    umount();
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    int fh = getNewFileHandle();
    fi.fh.set(fh);
    FSDataOutputStream output = getOutputStream(fh);
    if (output == null) {
      Path p = getPath(path);
      try {
        FileSystem fileSystem = p.getFileSystem(_configuration);
        if (fileSystem.isDirectory(p)) {
          return -ErrorCodes.EISDIR();
        }
        output = fileSystem.create(p, false);
        openWriter(fh, output);
        return 0;
      } catch (Throwable t) {
        LOGGER.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    } else {
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int truncate(String path, @off_t long size) {
    try {
      Path p = getPath(path);
      FileSystem fileSystem = p.getFileSystem(_configuration);
      if (fileSystem.isDirectory(p)) {
        return -ErrorCodes.EISDIR();
      }
      // @TODO
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int getattr(String path, FileStat stat) {
    Path p = getPath(path);
    FileStatus fileStatus;
    try {
      FileSystem fileSystem = p.getFileSystem(_configuration);
      fileStatus = fileSystem.getFileStatus(p);
      setGroupOwner(fileSystem, fileStatus, stat);
    } catch (FileNotFoundException e) {
      return -ErrorCodes.ENOENT();
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    stat.st_mtim.tv_sec.set(fileStatus.getModificationTime() / 1000);
    stat.st_mtim.tv_nsec.set(0);
    int permissions = getPermissions(fileStatus);
    if (fileStatus.isDirectory()) {
      stat.st_mode.set(FileStat.S_IFDIR | permissions);
      stat.st_size.set(2);
      return 0;
    } else {

      stat.st_mode.set(FileStat.S_IFREG | permissions);
      stat.st_size.set(fileStatus.getLen());
      return 0;
    }
  }

  private int getPermissions(FileStatus fileStatus) {
    FsPermission permission = fileStatus.getPermission();
    FsAction userAction = permission.getUserAction();
    FsAction groupAction = permission.getGroupAction();
    FsAction otherAction = permission.getOtherAction();
    return (int) ((userAction.ordinal() * Math.pow(8, 2)) + (groupAction.ordinal() * Math.pow(8, 1))
        + otherAction.ordinal());
  }

  private void setGroupOwner(FileSystem fileSystem, FileStatus fileStatus, FileStat stat) {
    String group = fileStatus.getGroup();
    String owner = fileStatus.getOwner();
    int uid = getOwnerId(owner);
    int gid = getGroupId(group);
    stat.st_gid.set(gid);
    stat.st_uid.set(uid);
  }

  private int getGroupId(String group) {
    Integer id = _gids.get(group);
    if (id == null) {
      try {
        id = lookupGroup(group);
      } catch (Throwable t) {
        LOGGER.error("Unknown error looking up uid for file", t);
      }
      if (id == null) {
        id = _defaultGroupId;
      }
      _gids.put(group, id);
    }
    return id;
  }

  private Integer lookupGroup(String group) throws InterruptedException, IOException {
    ProcessBuilder builder = new ProcessBuilder("sudo", "getent", "group", group);
    Process process = builder.start();
    if (process.waitFor() == 0) {
      String result = IOUtils.toString(process.getInputStream(), "UTF-8");
      List<String> list = Splitter.on(':')
                                  .splitToList(result.trim());
      String id = list.get(2);
      return Integer.parseInt(id.trim());
    }
    return null;
  }

  private int getOwnerId(String owner) {
    Integer id = _uids.get(owner);
    if (id == null) {
      try {
        id = lookupOwner(owner);
      } catch (Throwable t) {
        LOGGER.error("Unknown error looking up uid for file", t);
      }
      if (id == null) {
        id = _defaultOwnerId;
      }
      _uids.put(owner, id);
    }
    return id;
  }

  private Integer lookupOwner(String owner) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder("sudo", "id", "-u", owner);
    Process process = builder.start();
    if (process.waitFor() == 0) {
      String id = IOUtils.toString(process.getInputStream(), "UTF-8");
      return Integer.parseInt(id.trim());
    }
    return null;
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    Path p = getPath(path);
    try {
      FileSystem fileSystem = p.getFileSystem(_configuration);
      if (!fileSystem.isDirectory(p)) {
        return -ErrorCodes.ENOTDIR();
      }
      filter.apply(buf, CURRENT_DIR, null, 0);
      filter.apply(buf, PARENT_DIR, null, 0);
      FileStatus[] listStatus = fileSystem.listStatus(p);

      for (FileStatus fileStatus : listStatus) {
        String name = fileStatus.getPath()
                                .getName();
        filter.apply(buf, name, null, 0);
      }
      return 0;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    LOGGER.debug("read {} {} {} {} {}", path, size, offset, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    FSDataInputStream input = getInputStream(fh);
    if (input == null) {
      Path p = getPath(path);
      try {
        FileSystem fileSystem = p.getFileSystem(_configuration);
        if (fileSystem.isDirectory(p)) {
          return -ErrorCodes.EISDIR();
        }
        input = fileSystem.open(p);
        openReader(fh, input);
      } catch (Throwable t) {
        LOGGER.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    }
    try {
      int len = (int) size;
      byte[] buffer = new byte[len];
      if (input.getPos() != offset) {
        input.seek(offset);
      }
      int read = input.read(buffer);
      if (read == -1) {
        read = 0;
      }
      buf.put(0, buffer, 0, read);
      return read;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    LOGGER.debug("write {} {} {}", path, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    FSDataOutputStream output = getOutputStream(fh);
    if (output == null) {
      Path p = getPath(path);
      try {
        FileSystem fileSystem = p.getFileSystem(_configuration);
        if (fileSystem.isDirectory(p)) {
          return -ErrorCodes.EISDIR();
        }
        output = fileSystem.create(p, false);
        openWriter(fh, output);
      } catch (Throwable t) {
        LOGGER.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    }
    try {
      int len = (int) size;
      byte[] buffer = new byte[len];
      buf.get(0, buffer, 0, len);
      if (output.getPos() != offset) {
        return -ErrorCodes.EIO();
      }
      output.write(buffer);
      return len;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    LOGGER.debug("fsync {} {} {}", path, isdatasync, fi);
    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    fi.fh.set(getNewFileHandle());
    LOGGER.debug("open {} {} {}", path, fi, fi.fh.get());
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    LOGGER.debug("release {} {}", path, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    _handles.remove(fh);
    try {
      closeFileHandle(fh);
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    return fsync(path, 0, fi);
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    LOGGER.debug("flush {} {}", path, fi);
    return fsync(path, 0, fi);
  }

  private void openReader(int fh, FSDataInputStream inputStream) {
    _readers.put(fh, inputStream);
  }

  private void openWriter(int fh, FSDataOutputStream outputStream) {
    _writers.put(fh, outputStream);
  }

  private void closeFileHandle(int fh) throws IOException {
    closeReader(fh);
    closeWriter(fh);
  }

  private void closeReader(int fh) throws IOException {
    FSDataInputStream inputStream = _readers.remove(fh);
    if (inputStream != null) {
      inputStream.close();
    }
  }

  private void closeWriter(int fh) throws IOException {
    FSDataOutputStream outputStream = _writers.remove(fh);
    if (outputStream != null) {
      outputStream.close();
    }
  }

  private FSDataInputStream getInputStream(int fh) {
    return _readers.get(fh);
  }

  private FSDataOutputStream getOutputStream(int fh) {
    return _writers.get(fh);
  }

  private synchronized int getNewFileHandle() {
    while (true) {
      int fh = _fileHandleCounter.incrementAndGet();
      if (fh < 0) {
        _fileHandleCounter.set(1);
      } else if (!_handles.contains(fh)) {
        _handles.add(fh);
        return fh;
      }
    }
  }

}
