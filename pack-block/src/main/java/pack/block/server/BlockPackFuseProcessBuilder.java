package pack.block.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.util.Utils;

public class BlockPackFuseProcessBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuseProcessBuilder.class);

  private static final String BASH = "bash";
  private static final String PACK_LOG4J_CONFIG = "PACK_LOG4J_CONFIG";
  private static final String EXPORT = "export";
  private static final String SET_E = "set -e";
  private static final String SET_X = "set -x";
  private static final String PACK_LOG_DIR = "pack.log.dir";
  private static final String JAVA_PROPERTY = "-D";
  private static final String LOG4J_FUSE_PROCESS_XML = "log4j-fuse-process.xml";
  private static final String BACKGROUND = "&";
  private static final String STDERR_REDIRECT = "2>";
  private static final String STDOUT_REDIRECT = ">";
  private static final String STDERR = "/stderr";
  private static final String STDOUT = "/stdout";
  private static final String INHERENT_ENV_VAR_SWITCH = "-E";
  private static final String SUDO = "sudo";
  private static final String ENV = "env";
  private static final String BIN_BASH = "#!/bin/bash";
  private static final String START_SH = "start.sh";
  private static final String NOHUP = "/bin/nohup";
  private static final String JAVA_HOME = "java.home";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String BIN_JAVA = "/bin/java";
  private static final String XMX_SWITCH = "-Xmx128m";
  private static final String XMS_SWITCH = "-Xms128m";
  private static final String CLASSPATH_SWITCH = "-cp";
  private static final String DOCKER_UNIX_SOCKET = "docker.unix.socket";

  public static void startProcess(boolean nohupProcess, String fuseMountLocation, String fsMountLocation,
      String fsMetricsLocation, String fsLocalCache, String hdfsVolumePath, String zkConnection, int zkTimeout,
      String volumeName, String logOutput, String unixSock, String libDir, int numberOfMountSnapshots,
      long volumeMissingPollingPeriod, int volumeMissingCountBeforeAutoShutdown, boolean countDockerDownAsMissing,
      List<String> classPathExtras, boolean fileSystemMount) throws IOException {
    String javaHome = System.getProperty(JAVA_HOME);

    String classPath = buildClassPath(System.getProperty(JAVA_CLASS_PATH), libDir, classPathExtras);
    Builder<String> builder = ImmutableList.builder();

    String dockerUnixSocket = System.getProperty(DOCKER_UNIX_SOCKET);

    String zkTimeoutStr = Integer.toString(zkTimeout);
    if (nohupProcess) {
      builder.add(NOHUP);
    } else {
      builder.add("exec");
    }

    builder.add(javaHome + BIN_JAVA)
           .add(XMX_SWITCH)
           .add(XMS_SWITCH)
           .add(JAVA_PROPERTY + PACK_LOG_DIR + "=" + logOutput);
    if (dockerUnixSocket != null) {
      builder.add(JAVA_PROPERTY + DOCKER_UNIX_SOCKET + "=" + dockerUnixSocket);
    }
    builder.add(CLASSPATH_SWITCH)
           .add(classPath)
           .add(BlockPackFuse.class.getName())
           .add(volumeName)
           .add(fuseMountLocation)
           .add(fsMountLocation)
           .add(fsMetricsLocation)
           .add(fsLocalCache)
           .add(hdfsVolumePath)
           .add(zkConnection)
           .add(zkTimeoutStr)
           .add(unixSock)
           .add(Integer.toString(numberOfMountSnapshots))
           .add(Long.toString(volumeMissingPollingPeriod))
           .add(Integer.toString(volumeMissingCountBeforeAutoShutdown))
           .add(Boolean.toString(countDockerDownAsMissing))
           .add(Boolean.toString(fileSystemMount))
           .add(STDOUT_REDIRECT + logOutput + STDOUT)
           .add(STDERR_REDIRECT + logOutput + STDERR);
    if (nohupProcess) {
      builder.add(BACKGROUND);
    }
    ImmutableList<String> build = builder.build();
    String cmd = Joiner.on(' ')
                       .join(build);
    File logConfig = new File(logOutput, LOG4J_FUSE_PROCESS_XML);
    File start = new File(logOutput, START_SH);
    File target = new File(new File(fsLocalCache).getParentFile(), "log");
    try (PrintWriter output = new PrintWriter(start)) {
      output.println(BIN_BASH);
      output.println(SET_X);
      output.println(SET_E);
      output.println(ENV);
      output.println("rm -f " + target.getAbsolutePath());
      output.println("ln -s " + logConfig.getParentFile()
                                         .getAbsolutePath()
          + " " + target.getAbsolutePath());
      output.println(EXPORT + " " + PACK_LOG4J_CONFIG + "=" + logConfig.getAbsolutePath());
      IOUtils.write(cmd, output);
      output.println();
    }
    if (!logConfig.exists()) {
      try (InputStream inputStream = BlockPackFuse.class.getResourceAsStream("/" + LOG4J_FUSE_PROCESS_XML)) {
        try (FileOutputStream outputStream = new FileOutputStream(logConfig)) {
          IOUtils.copy(inputStream, outputStream);
        }
      }
    }

    LOGGER.info("Starting fuse mount from script file {}", start.getAbsolutePath());
    Utils.exec(LOGGER, SUDO, INHERENT_ENV_VAR_SWITCH, BASH, "-x", start.getAbsolutePath());
  }

  private static String buildClassPath(String classPathProperty, String libDir, List<String> classPathExtras)
      throws IOException {
    Utils.rmr(new File(libDir));
    Builder<String> builder = ImmutableList.builder();
    if (classPathExtras != null) {
      for (String cpe : classPathExtras) {
        copyFiles(libDir, builder, cpe);
      }
    }
    List<String> classPath = Splitter.on(':')
                                     .splitToList(classPathProperty);
    for (String file : classPath) {
      copyFiles(libDir, builder, file);
    }
    return Joiner.on(':')
                 .join(builder.build());
  }

  private static void copyFiles(String libDir, Builder<String> builder, String file) throws IOException {
    File src = new File(file);
    File dest = new File(libDir, src.getName());
    if (src.exists()) {
      copy(src, dest);
      builder.add(dest.getAbsolutePath());
    }
  }

  private static void copy(File src, File dest) throws IOException {
    if (src.isDirectory()) {
      dest.mkdirs();
      for (File f : src.listFiles()) {
        copy(f, new File(dest, f.getName()));
      }
    } else {
      try (InputStream input = new BufferedInputStream(new FileInputStream(src))) {
        dest.delete();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(dest))) {
          IOUtils.copy(input, output);
        }
      }
    }
  }
}
