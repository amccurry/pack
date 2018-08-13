package pack.block.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import pack.block.server.json.BlockPackFuseConfig;
import pack.block.util.Utils;

public class BlockPackFuseProcessBuilder {

  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";

  private static final Logger LOGGER = LoggerFactory.getLogger(BlockPackFuseProcessBuilder.class);

  private static final String PACK_VOLUME = "pack-volume";
  private static final String EXEC_NAME = "-a";
  private static final String EXEC = "exec";
  private static final String _JAVA_OPTIONS = "_JAVA_OPTIONS";
  private static final String CLASSPATH = "CLASSPATH";
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
  private static final String XX_USE_G1GC = "-XX:+UseG1GC";
  private static final String LOG = "log";
  private static final String RUN_SH = "run.sh";
  private static final String HADOOP_CONFIG = "hadoop-config";
  private static final String HDFS_SITE_XML = "hdfs-site.xml";
  private static final String CORE_SITE_XML = "core-site.xml";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void startProcess(String volumeName, String workingDir, String logDir, String libDir,
      String configFilePath, Configuration configuration, BlockPackFuseConfig config) throws IOException {

    String javaHome = System.getProperty(JAVA_HOME);

    String classPathLibs = buildClassPath(System.getProperty(JAVA_CLASS_PATH), libDir, null);
    File configDir = new File(libDir, HADOOP_CONFIG);
    copyConfig(configuration, configDir);
    String classPath = Joiner.on(':')
                             .join(configDir.getCanonicalPath(), classPathLibs);

    File configFile = new File(configFilePath);

    MAPPER.writeValue(configFile, config);
    Builder<String> builder = ImmutableList.builder();
    builder.add(EXEC)
           .add(EXEC_NAME)
           .add(PACK_VOLUME)
           .add(javaHome + BIN_JAVA)
           .add(BlockPackFuse.class.getName())
           .add(configFile.getAbsolutePath())
           .add(STDOUT_REDIRECT + logDir + STDOUT)
           .add(STDERR_REDIRECT + logDir + STDERR);

    String cmd = Joiner.on(' ')
                       .join(builder.build());
    File logConfig = new File(workingDir, LOG4J_FUSE_PROCESS_XML);
    File start = new File(workingDir, START_SH);
    File target = new File(workingDir, LOG);
    try (PrintWriter output = new PrintWriter(start)) {
      output.println(BIN_BASH);
      output.println(SET_X);
      output.println(SET_E);

      if (!UserGroupInformation.isSecurityEnabled()) {
        output.println(EXPORT + " " + HADOOP_USER_NAME + "=" + Utils.getUserGroupInformation()
                                                                    .getUserName());
      }

      output.println("rm -f " + target.getAbsolutePath());
      output.println("ln -s " + logDir + " " + target.getAbsolutePath());
      output.println(EXPORT + " " + PACK_LOG4J_CONFIG + "=" + logConfig.getAbsolutePath());
      output.println(EXPORT + " " + CLASSPATH + "=" + classPath);

      Builder<String> javaOptsBuilder = ImmutableList.builder();
      javaOptsBuilder.add(toProp(PACK_LOG_DIR, logDir))
                     .add(XX_USE_G1GC)
                     .add(XMX_SWITCH)
                     .add(XMS_SWITCH);
      String javaOpts = Joiner.on(' ')
                              .join(javaOptsBuilder.build());
      output.println(EXPORT + " " + _JAVA_OPTIONS + "=\"" + javaOpts + "\"");
      output.println(ENV);
      File run = new File(workingDir, RUN_SH);
      output.println("chmod +x " + run.getAbsolutePath());
      try (PrintWriter runWriter = new PrintWriter(run)) {
        runWriter.println(BIN_BASH);
        runWriter.println(SET_X);
        runWriter.println(SET_E);

        String hdfsPrinciaplName = Utils.getHdfsPrincipalName();
        if (hdfsPrinciaplName != null) {
          // Copy keytab in case where source file is destroyed
          String hdfsKeytab = Utils.getHdfsKeytab();
          File src = new File(hdfsKeytab);
          if (!src.exists()) {
            throw new FileNotFoundException(src.getAbsolutePath());
          }
          File dstDir = new File(workingDir);
          dstDir.mkdirs();
          File dst = new File(dstDir, src.getName());
          copyFile(src, dst);
          String newKeytabPath = dst.getAbsolutePath();
          runWriter.println("export " + Utils.PACK_HDFS_KERBEROS_KEYTAB + "=" + newKeytabPath);
          runWriter.println("export " + Utils.PACK_HDFS_KERBEROS_PRINCIPAL_NAME + "=" + hdfsPrinciaplName);
        }
        runWriter.println(cmd);
      }
      ImmutableList<Object> command = ImmutableList.builder()
                                                   .add(NOHUP)
                                                   .add(run.getAbsolutePath())
                                                   .add(STDOUT_REDIRECT + logDir + STDOUT + ".run")
                                                   .add(STDERR_REDIRECT + logDir + STDERR + ".run")
                                                   .add(BACKGROUND)
                                                   .build();
      output.println(Joiner.on(' ')
                           .join(command));
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

  private static void copyConfig(Configuration configuration, File configDir) throws IOException {
    configDir.mkdirs();
    writeFile(configuration, configDir, CORE_SITE_XML);
    writeFile(configuration, configDir, HDFS_SITE_XML);
  }

  private static void writeFile(Configuration configuration, File configDir, String fileName)
      throws IOException, FileNotFoundException {
    try (OutputStream out = new FileOutputStream(new File(configDir, fileName))) {
      configuration.writeXml(out);
    }
  }

  private static void copyFile(File src, File dst) throws IOException {
    try (InputStream input = new FileInputStream(src)) {
      try (FileOutputStream output = new FileOutputStream(dst)) {
        IOUtils.copy(input, output);
      }
    }
  }

  private static String toProp(String name, String value) {
    return JAVA_PROPERTY + name + "=" + value;
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
