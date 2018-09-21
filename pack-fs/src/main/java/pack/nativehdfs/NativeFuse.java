package pack.nativehdfs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.ffi.Pointer;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnrfuse.ErrorCodes;
import jnrfuse.FuseFillDir;
import jnrfuse.FuseStubFS;
import jnrfuse.struct.FileStat;
import jnrfuse.struct.Flock;
import jnrfuse.struct.FuseFileInfo;

public class NativeFuse extends FuseStubFS implements Closeable {
  private static final String NONEMPTY = "nonempty";

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFuse.class);

  public static final String BRICK = "brick";
  public static final String FUSE_PID = "fuse_pid";
  private static final String SYNC = "sync";
  private static final String PARENT_DIR = "..";
  private static final String CURRENT_DIR = ".";
  private static final String OPTION_SWITCH = "-o";
  private static final String AUTO_UNMOUNT = "auto_unmount";
  private static final String ALLOW_ROOT = "allow_root";

  private final File _mountPath;
  private final File _fsPath;
  private final Set<Integer> _handles = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private final AtomicInteger _handleCounter = new AtomicInteger(1);
  private final Map<Integer, FileReaderWriter> _files = new ConcurrentHashMap<>();

  public NativeFuse(File mountPath, File fsPath) throws IOException {
    _mountPath = mountPath.getCanonicalFile();
    _fsPath = fsPath.getCanonicalFile();
  }

  public void localMount() {
    try {
      localMount(true);
    } catch (Throwable t) {
      LOGGER.error("Unknown error during fuse mount", t);
    }
  }

  public void localMount(boolean blocking) throws IOException {
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
    mount(Paths.get(_mountPath.getCanonicalPath()), blocking, true, opts);
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("close");
    umount();
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    int fh = generateNewFh();
    fi.fh.set(fh);
    try {
      FileReaderWriter file = new FileReaderWriter(getRealPath(path));
      _files.put((int) fh, file);
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    LOGGER.debug("open {} {} {}", path, fi, fi.fh.get());
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    LOGGER.debug("release {} {}", path, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    _handles.remove(fh);
    try {
      closeFile(fh);
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
    return fsync(path, 0, fi);
  }

  @Override
  public int rename(String oldpath, String newpath) {
    File realPath = getRealPath(oldpath);
    if (!realPath.exists()) {
      return -ErrorCodes.EEXIST();
    }
    File dest = getRealPath(newpath);
    return realPath.renameTo(dest) ? 0 : 1;
  }

  @Override
  public int unlink(String path) {
    File realPath = getRealPath(path);
    if (!realPath.exists()) {
      return -ErrorCodes.EEXIST();
    }
    if (realPath.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }
    return realPath.delete() ? 0 : 1;
  }

  @Override
  public int rmdir(String path) {
    File realPath = getRealPath(path);
    if (!realPath.exists()) {
      return -ErrorCodes.EEXIST();
    }
    if (!realPath.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }
    return realPath.delete() ? 0 : 1;
  }

  @Override
  public int create(String path, long mode, FuseFileInfo fi) {
    // check mode ???
    File realPath = getRealPath(path);
    if (realPath.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }
    if (realPath.exists()) {
      return -ErrorCodes.EEXIST();
    }
    try {
      return open(path, fi);
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int mkdir(String path, long mode) {
    File realPath = getRealPath(path);
    if (realPath.exists()) {
      return -ErrorCodes.EEXIST();
    }
    int returnVal = realPath.mkdir() ? 0 : 1;
    if (returnVal == 0) {
      try {
        Files.setPosixFilePermissions(realPath.toPath(), getPermissions(mode));
      } catch (Throwable t) {
        LOGGER.error("Unknown error.", t);
        return -ErrorCodes.EIO();
      }
    }
    return returnVal;
  }

  @Override
  public int truncate(String path, @off_t long size) {
    File realPath = getRealPath(path);
    if (realPath.isDirectory()) {
      return -ErrorCodes.EISDIR();
    }
    try (FileReaderWriter file = new FileReaderWriter(getRealPath(path))) {
      file.setLength(size);
      return 0;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int getattr(String path, FileStat stat) {
    try {
      File realPath = getRealPath(path);
      if (!realPath.exists()) {
        return -ErrorCodes.ENOENT();
      }

      Path p = realPath.toPath();
      PosixFileAttributeView posixView = Files.getFileAttributeView(p, PosixFileAttributeView.class);
      PosixFileAttributes attributes = posixView.readAttributes();
      UserPrincipal owner = attributes.owner();
      GroupPrincipal group = attributes.group();
      Set<PosixFilePermission> permissionsSet = attributes.permissions();

      int permissions = getPermissions(permissionsSet);
      stat.st_mtim.tv_sec.set(realPath.lastModified() / 1000);
      stat.st_mtim.tv_nsec.set(0);
      stat.st_gid.set(getGid(group));
      stat.st_uid.set(getUid(owner));
      if (realPath.isDirectory()) {
        stat.st_mode.set(FileStat.S_IFDIR | permissions);
        stat.st_size.set(2);
        return 0;
      } else {
        stat.st_mode.set(FileStat.S_IFREG | permissions);
        stat.st_size.set(realPath.length());
        return 0;
      }
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  private Set<PosixFilePermission> getPermissions(long mode) {
    Set<PosixFilePermission> result = new HashSet<>();
    if ((mode & FileStat.S_IROTH) == FileStat.S_IROTH) {
      result.add(PosixFilePermission.OTHERS_READ);
    }
    if ((mode & FileStat.S_IWOTH) == FileStat.S_IWOTH) {
      result.add(PosixFilePermission.OTHERS_WRITE);
    }
    if ((mode & FileStat.S_IXOTH) == FileStat.S_IXOTH) {
      result.add(PosixFilePermission.OTHERS_EXECUTE);
    }
    if ((mode & FileStat.S_IRGRP) == FileStat.S_IRGRP) {
      result.add(PosixFilePermission.GROUP_READ);
    }
    if ((mode & FileStat.S_IWGRP) == FileStat.S_IWGRP) {
      result.add(PosixFilePermission.GROUP_WRITE);
    }
    if ((mode & FileStat.S_IXGRP) == FileStat.S_IXGRP) {
      result.add(PosixFilePermission.GROUP_EXECUTE);
    }
    if ((mode & FileStat.S_IRUSR) == FileStat.S_IRUSR) {
      result.add(PosixFilePermission.OWNER_READ);
    }
    if ((mode & FileStat.S_IWUSR) == FileStat.S_IWUSR) {
      result.add(PosixFilePermission.OWNER_WRITE);
    }
    if ((mode & FileStat.S_IXUSR) == FileStat.S_IXUSR) {
      result.add(PosixFilePermission.OWNER_EXECUTE);
    }
    return result;
  }

  private static int getPermissions(Set<PosixFilePermission> permissionsSet) {
    int permission = 0;
    for (PosixFilePermission filePermission : permissionsSet) {
      switch (filePermission) {
      case OWNER_READ:
        permission = (permission | FileStat.S_IRUSR);
        break;
      case OWNER_WRITE:
        permission = (permission | FileStat.S_IWUSR);
        break;
      case OWNER_EXECUTE:
        permission = (permission | FileStat.S_IXUSR);
        break;
      case GROUP_READ:
        permission = (permission | FileStat.S_IRGRP);
        break;
      case GROUP_WRITE:
        permission = (permission | FileStat.S_IWGRP);
        break;
      case GROUP_EXECUTE:
        permission = (permission | FileStat.S_IXGRP);
        break;
      case OTHERS_READ:
        permission = (permission | FileStat.S_IROTH);
        break;
      case OTHERS_WRITE:
        permission = (permission | FileStat.S_IWOTH);
        break;
      case OTHERS_EXECUTE:
        permission = (permission | FileStat.S_IXOTH);
        break;
      default:
        break;
      }
    }
    return permission;
  }

  private static Number getUid(UserPrincipal owner) throws Exception {
    Field field = owner.getClass()
                       .getDeclaredField("id");
    field.setAccessible(true);
    return (Number) field.get(owner);
  }

  private static Number getGid(GroupPrincipal group) throws Exception {
    Field field = group.getClass()
                       .getSuperclass()
                       .getDeclaredField("id");
    field.setAccessible(true);
    return (Number) field.get(group);
  }

  @Override
  public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
    File realPath = getRealPath(path);
    if (!realPath.exists()) {
      return -ErrorCodes.ENOENT();
    }
    if (!realPath.isDirectory()) {
      return -ErrorCodes.ENOTDIR();
    }
    filter.apply(buf, CURRENT_DIR, null, 0);
    filter.apply(buf, PARENT_DIR, null, 0);
    for (File file : realPath.listFiles()) {
      filter.apply(buf, file.getName(), null, 0);

    }
    return 0;
  }

  @Override
  public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    LOGGER.debug("read {} {} {} {} {}", path, size, offset, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    if (fh == 0) {
      int open = open(path, fi);
      if (open != 0) {
        return open;
      }
    }
    FileReaderWriter file = _files.get(fh);
    try {
      return file.read(buf, size, offset);
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    LOGGER.debug("write {} {} {}", path, fi, fi.fh.get());
    int fh = (int) fi.fh.get();
    if (fh == 0) {
      int open = open(path, fi);
      if (open != 0) {
        return open;
      }
    }
    FileReaderWriter file = _files.get(fh);
    try {
      return file.write(buf, size, offset);
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
  public int flush(String path, FuseFileInfo fi) {
    LOGGER.debug("flush {} {}", path, fi);
    return 0;
  }

  @Override
  public int lock(String path, FuseFileInfo fi, int cmd, Flock flock) {
    // TODO Auto-generated method stub
    return super.lock(path, fi, cmd, flock);
  }

  @Override
  public int readlink(String path, Pointer buf, long size) {
    // TODO Auto-generated method stub
    return super.readlink(path, buf, size);
  }

  @Override
  public int symlink(String oldpath, String newpath) {
    // TODO Auto-generated method stub
    return super.symlink(oldpath, newpath);
  }

  @Override
  public int link(String oldpath, String newpath) {
    // TODO Auto-generated method stub
    return super.link(oldpath, newpath);
  }

  @Override
  public int chmod(String path, long mode) {
    File realPath = getRealPath(path);
    if (!realPath.exists()) {
      return -ErrorCodes.ENOENT();
    }
    try {
      Files.setPosixFilePermissions(realPath.toPath(), getPermissions(mode));
      return 0;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int chown(String path, long uid, long gid) {
    File realPath = getRealPath(path);
    if (!realPath.exists()) {
      return -ErrorCodes.ENOENT();
    }
    try {
      int exec = exec("sudo", "chown", uid + ":" + gid, realPath.getAbsolutePath());
      return exec;
    } catch (Throwable t) {
      LOGGER.error("Unknown error.", t);
      return -ErrorCodes.EIO();
    }
  }

  private int exec(String... args) throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(args);
    Process process = builder.start();
    return process.waitFor();
  }

  private synchronized int generateNewFh() {
    while (true) {
      int handle = _handleCounter.incrementAndGet();
      if (!_handles.contains(handle)) {
        _handles.add(handle);
        return handle;
      }
    }
  }

  private File getRealPath(String path) {
    return new File(_fsPath, path);
  }

  private void closeFile(int fh) throws IOException {
    FileReaderWriter writer = _files.remove(fh);
    if (writer != null) {
      writer.close();
    }
  }
}
