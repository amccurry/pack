package pack;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;

public class TarPackStorage implements PackStorage {

  private static final Logger LOG = LoggerFactory.getLogger(TarPackStorage.class);

  private static final String TAR_GZ = ".tar.gz";
  private static final String VOLUME_TAR_GZ = "volume.tar.gz";

  private final Configuration configuration;
  private final Path root;
  private final UserGroupInformation ugi;
  private final File localFile;
  private final Map<String, File> mounts = new MapMaker().makeMap();
  private final int maxOldFiles;

  public TarPackStorage(File localFile, Configuration configuration, Path remotePath, UserGroupInformation ugi) {
    this.localFile = localFile;
    this.configuration = configuration;
    this.root = remotePath;
    this.ugi = ugi;
    maxOldFiles = 5;
  }

  @Override
  public void create(String volumeName, Map<String, Object> options) throws Exception {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        LOG.info("Create Volume {}", volumeName);
        Path volumePath = new Path(root, volumeName);
        FileSystem fileSystem = getFileSystem(volumePath);
        if (fileSystem.exists(volumePath)) {
          throw new RuntimeException("Volume " + volumeName + " already exists.");
        }
        fileSystem.mkdirs(volumePath);
        return null;
      }
    });
  }

  @Override
  public void remove(String volumeName) throws Exception {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        LOG.info("Remove Volume {}", volumeName);
        Path volumePath = new Path(root, volumeName);
        FileSystem fileSystem = getFileSystem(volumePath);
        fileSystem.delete(volumePath, true);
        return null;
      }
    });
  }

  @Override
  public String mount(String volumeName, String id) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        LOG.info("Mount Volume {} Id {}", volumeName, id);
        Path volumePath = new Path(root, volumeName);
        FileSystem fileSystem = getFileSystem(volumePath);
        if (!fileSystem.exists(volumePath)) {
          throw new RuntimeException("Volume " + volumeName + " does not exist.");
        }
        File mountFile = new File(localFile, id);
        mountFile.mkdirs();
        LOG.info("Local Volume Dir {} ", mountFile);
        mounts.put(volumeName, mountFile);
        File file = new File(localFile, id + TAR_GZ);
        if (file.exists()) {
          file.delete();
        }
        Path path = getExistingVolumePath(volumeName, fileSystem);
        if (path != null && fileSystem.exists(path)) {
          try (InputStream inputStream = fileSystem.open(path)) {
            try (OutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
              IOUtils.copy(inputStream, output);
            }
          }
          LOG.info("Download of Tar Complete {} ", file);
          ProcessBuilder builder = new ProcessBuilder(Arrays.asList("/usr/bin/tar", "-xpvzf", file.getAbsolutePath()));
          builder.directory(mountFile);
          Process process = builder.start();
          read("stdout", process.getInputStream());
          read("stderr", process.getErrorStream());
          if (process.waitFor() != 0) {
            throw new RuntimeException("Unknown error");
          }
          LOG.info("Extraction of Tar Complete {} ", file);
          file.delete();
        }
        return mountFile.getAbsolutePath();
      }

    });
  }

  @Override
  public void unmount(String volumeName, String id) throws Exception {
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        LOG.info("Unmount Volume {} Id {}", volumeName, id);
        Path volumePath = new Path(root, volumeName);
        FileSystem fileSystem = getFileSystem(volumePath);
        if (!fileSystem.exists(volumePath)) {
          throw new RuntimeException("Volume " + volumeName + " does not exist.");
        }
        File mountFile = mounts.get(volumeName);
        File file = new File(localFile, id + TAR_GZ);
        LOG.info("Local Volume Dir {} ", mountFile);
        // tar -cvzf out.tar.gx ./
        ProcessBuilder builder = new ProcessBuilder(
            Arrays.asList("/usr/bin/tar", "-cpvzf", file.getAbsolutePath(), "./"));
        builder.directory(mountFile);
        Process process = builder.start();
        read("stdout", process.getInputStream());
        read("stderr", process.getErrorStream());
        if (process.waitFor() != 0) {
          throw new RuntimeException("Unknown error");
        }
        LOG.info("Packing Volume Complete {} ", file);

        Path path = getNewVolumePath(volumeName, fileSystem);
        try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
          try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
            IOUtils.copy(input, outputStream);
          }
        }

        LOG.info("Upload Volume Complete {} ", path);

        FileUtils.deleteDirectory(mountFile);
        file.delete();
        cleanupOldVolumes(fileSystem, volumePath);
        return null;
      }
    });
  }

  protected void cleanupOldVolumes(FileSystem fileSystem, Path volumePath) throws IOException {
    FileStatus[] listStatus = getOrderedFileStatus(fileSystem, volumePath);
    for (int i = maxOldFiles; i < listStatus.length; i++) {
      Path path = listStatus[i].getPath();
      LOG.info("Removing old volume file {}", path);
      fileSystem.delete(path, false);
    }
  }

  @Override
  public boolean exists(String volumeName) throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<Boolean>() {
      @Override
      public Boolean run() throws Exception {
        LOG.info("exists {}", volumeName);
        FileSystem fileSystem = getFileSystem(root);
        return fileSystem.exists(new Path(root, volumeName));
      }
    });
  }

  @Override
  public String getMountPoint(String volumeName) {
    File file = mounts.get(volumeName);
    LOG.info("Get MountPoint volume {} path {}", volumeName, file);
    if (file == null) {
      return null;
    }
    return file.getAbsolutePath();
  }

  @Override
  public List<String> listVolumes() throws Exception {
    return ugi.doAs(new PrivilegedExceptionAction<List<String>>() {
      @Override
      public List<String> run() throws Exception {
        LOG.info("List Volumes");
        FileSystem fileSystem = getFileSystem(root);
        FileStatus[] listStatus = fileSystem.listStatus(root);
        List<String> result = new ArrayList<>();
        for (FileStatus fileStatus : listStatus) {
          result.add(fileStatus.getPath()
                               .getName());
        }
        return result;
      }
    });
  }

  private FileSystem getFileSystem(Path volumePath) throws IOException {
    return volumePath.getFileSystem(configuration);
  }

  private void read(String type, InputStream inputStream) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
          String line;
          while ((line = reader.readLine()) != null) {
            LOG.info("Process " + type + " - " + line);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  private Path getNewVolumePath(String volumeName, FileSystem fileSystem) throws IOException {
    Path dir = new Path(root, volumeName);
    Long gen = getGeneration(fileSystem, dir);
    if (gen == null) {
      return new Path(dir, VOLUME_TAR_GZ + ".0");
    } else {
      return new Path(dir, VOLUME_TAR_GZ + "." + (gen + 1));
    }
  }

  private Path getExistingVolumePath(String volumeName, FileSystem fileSystem) throws IOException {
    Path dir = new Path(root, volumeName);
    Long gen = getGeneration(fileSystem, dir);
    if (gen == null) {
      return null;
    }
    return new Path(dir, VOLUME_TAR_GZ + "." + gen);
  }

  private static final Comparator<? super FileStatus> COMPARATOR = (o1, o2) -> {
    Long gen1 = getGen(o1.getPath());
    Long gen2 = getGen(o2.getPath());
    if (gen1 == null) {
      return 1;
    } else if (gen2 == null) {
      return -1;
    } else {
      return Long.compare(gen2, gen1);
    }
  };

  private static Long getGeneration(FileSystem fileSystem, Path dir) throws FileNotFoundException, IOException {
    FileStatus[] listStatus = getOrderedFileStatus(fileSystem, dir);
    if (listStatus == null || listStatus.length == 0) {
      return null;
    }
    return getGen(listStatus[0].getPath());
  }

  private static FileStatus[] getOrderedFileStatus(FileSystem fileSystem, Path dir)
      throws IOException, FileNotFoundException {
    FileStatus[] listStatus = null;
    if (fileSystem.exists(dir)) {
      listStatus = fileSystem.listStatus(dir, (PathFilter) path -> path.getName()
                                                                       .startsWith(VOLUME_TAR_GZ));
      Arrays.sort(listStatus, COMPARATOR);
    }
    return listStatus;
  }

  private static Long getGen(Path path) {
    String n = path.getName();
    int lastIndexOf = n.lastIndexOf('.');
    try {
      return Long.parseLong(n.substring(lastIndexOf + 1));
    } catch (NumberFormatException e) {
      return null;
    }
  }

}
