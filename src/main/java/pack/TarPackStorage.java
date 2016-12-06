package pack;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  public TarPackStorage(File localFile, Configuration configuration, Path remotePath, UserGroupInformation ugi) {
    this.localFile = localFile;
    this.configuration = configuration;
    this.root = remotePath;
    this.ugi = ugi;
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
        Path path = new Path(new Path(root, volumeName), VOLUME_TAR_GZ);
        if (fileSystem.exists(path)) {
          try (InputStream inputStream = fileSystem.open(path)) {
            try (OutputStream output = new BufferedOutputStream(new FileOutputStream(file))) {
              IOUtils.copy(inputStream, output);
            }
          }
          LOG.info("Download of Tar Complete {} ", file);
          ProcessBuilder builder = new ProcessBuilder(Arrays.asList("/usr/bin/tar", "-xpvzf", file.getAbsolutePath()));
          builder.directory(mountFile);
          Process process = builder.start();
          read(process.getInputStream());
          read(process.getErrorStream());
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
        read(process.getInputStream());
        read(process.getErrorStream());
        if (process.waitFor() != 0) {
          throw new RuntimeException("Unknown error");
        }
        LOG.info("Packing Volume Complete {} ", file);

        Path path = new Path(new Path(root, volumeName), VOLUME_TAR_GZ);
        try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
          try (InputStream input = new BufferedInputStream(new FileInputStream(file))) {
            IOUtils.copy(input, outputStream);
          }
        }

        LOG.info("Upload Volume Complete {} ", path);

        FileUtils.deleteDirectory(mountFile);
        file.delete();
        return null;
      }
    });
  }

  @Override
  public String getMountPoint(String volumeName) {
    File file = mounts.get(volumeName);
    LOG.info("Get MountPoint {}", file);
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
          result.add(fileStatus.getPath().getName());
        }
        return result;
      }
    });
  }

  private FileSystem getFileSystem(Path volumePath) throws IOException {
    return volumePath.getFileSystem(configuration);
  }

  private void read(InputStream inputStream) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          IOUtils.copy(inputStream, System.out);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
  }
}
