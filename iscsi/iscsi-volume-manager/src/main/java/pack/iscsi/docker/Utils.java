package pack.iscsi.docker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pack.distributed.storage.http.TargetServerInfo;

public class Utils {

  private static final String ISCSIADM = "iscsiadm";
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static String getEnv(String env) {
    String str = System.getenv(env);
    if (str == null) {
      throw new RuntimeException("Env " + env + " missing.");
    }
    return str;
  }

  public static Result execAsResult(Logger logger, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    logger.info("Executing command id {} cmd {}", uuid, list);
    try {
      return exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      logger.info("Command id {} complete", uuid);
    }
  }

  public static class Result {
    public final int exitCode;
    public final String stdout;
    public final String stderr;

    public Result(int exitCode, String stdout, String stderr) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }

  public static Result exec(String cmdId, List<String> command, Logger logger)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    Process process = builder.start();
    StringWriter stdout = new StringWriter();
    StringWriter stderr = new StringWriter();
    Thread t1 = captureOutput(cmdId, "stdout", toBuffer(process.getInputStream()), logger, stdout);
    Thread t2 = captureOutput(cmdId, "stderr", toBuffer(process.getErrorStream()), logger, stderr);
    t1.start();
    t2.start();
    int exitCode = process.waitFor();
    t1.join();
    t2.join();
    return new Result(exitCode, stdout.toString(), stderr.toString());
  }

  private static BufferedReader toBuffer(InputStream inputStream) {
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  private static Thread captureOutput(String cmdId, String type, BufferedReader reader, Logger logger, Writer writer) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String s;
          while ((s = reader.readLine()) != null) {
            logger.info("Command {} Type {} Message {}", cmdId, type, s.trim());
            writer.write(s);
            writer.write('\n');
          }
        } catch (IOException e) {
          LOGGER.error("Unknown error", e);
        } finally {
          try {
            writer.close();
          } catch (IOException e) {
            LOGGER.error("Error trying to close output writer", e);
          }
        }
      }
    });
  }

  public static String getMultiPathDevice(List<TargetServerInfo> list, String iqn)
      throws IOException, InterruptedException {
    File file = new File("/dev/disk/by-path");
    while (!file.exists()) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    }
    List<String> allMapperDevices = new ArrayList<>();
    for (File f : file.listFiles()) {
      if (f.getAbsolutePath()
           .contains("iscsi-" + iqn + "-lun-0")) {
        String multipathName;
        while ((multipathName = getMultipathName(f)) == null) {
          Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        }
        allMapperDevices.add(multipathName);

      }
    }
    String device = checkMultipathEntries(iqn, allMapperDevices);
    return device;
  }

  public static String checkMultipathEntries(String iqn, List<String> allMapperDevices) throws IOException {
    if (allMapperDevices.isEmpty()) {
      throw new IOException("No multipath device found for iqn " + iqn);
    }
    String device = allMapperDevices.get(0);
    for (String d : allMapperDevices) {
      if (!d.equals(device)) {
        throw new IOException("Not all iscsi devices [" + iqn + "] point to the same multipath device");
      }
    }
    return device;
  }

  public static String getMultipathName(File f) throws IOException {
    Result result = Utils.execAsResult(LOGGER, "readlink", "-f", f.getAbsolutePath());
    String systemDevice;
    if (result.exitCode == 0) {
      systemDevice = result.stdout.trim();
    } else {
      throw new RuntimeException("Unknown error while trying to get system device for " + f.getAbsolutePath());
    }
    if (systemDevice == null) {
      return null;
    }
    File systemDeviceFile = new File(systemDevice);
    LOGGER.info("System device file {}", systemDeviceFile);
    String devName = systemDeviceFile.getName();

    File multipathSysDevFile = findMasterDevice(new File("/sys/block"), devName);
    LOGGER.info("Multipath System device file {}", multipathSysDevFile);
    if (multipathSysDevFile == null) {
      return null;
    }
    File multipathSysNameFile = new File(new File(multipathSysDevFile, "dm"), "name");
    if (!multipathSysNameFile.exists()) {
      return null;
    }
    String multipathName = readFile(multipathSysNameFile);
    LOGGER.info("Multipath device name {}", multipathName);
    return multipathName;
  }

  public static String readFile(File multipathSysNameFile) throws IOException {
    StringBuilder builder = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(multipathSysNameFile)))) {
      String line;
      while ((line = reader.readLine()) != null) {
        builder.append(line.trim());
      }
    }
    return builder.toString();
  }

  public static File findMasterDevice(File file, String devName) {
    File[] listFiles = file.listFiles();
    for (File dev : listFiles) {
      File f = new File(new File(dev, "slaves"), devName);
      if (f.exists()) {
        return dev;
      }
    }
    return null;
  }

  public static void iscsiLoginSession(String iqn) throws IOException {
    Result result = Utils.execAsResult(LOGGER, "sudo", ISCSIADM, "-m", "node", "-T", iqn, "-l");
    if (result.exitCode != 0) {
      LOGGER.error("Error during login to iqn {} {}", iqn, result.stderr);
    } else {
      LOGGER.info("Successful login to iqn {} {}", iqn);
    }
  }

  public static void iscsiLogoutSession(String iqn) throws IOException {
    Result result = Utils.execAsResult(LOGGER, "sudo", ISCSIADM, "-m", "node", "-T", iqn, "-u");
    if (result.exitCode != 0) {
      LOGGER.error("Error during logout to iqn {} {}", iqn, result.stderr);
    } else {
      LOGGER.info("Successful logout to iqn {} {}", iqn);
    }
  }

  public static void iscsiDeleteSession(String iqn) throws IOException {
    Result result = Utils.execAsResult(LOGGER, "sudo", ISCSIADM, "-m", "node", "-T", iqn, "-o", "delete");
    if (result.exitCode != 0) {
      LOGGER.error("Error during logout to iqn {} {}", iqn, result.stderr);
    } else {
      LOGGER.info("Successful logout to iqn {} {}", iqn);
    }
  }

  public static void waitUntilBlockDeviceIsOnline(String dev) throws IOException, InterruptedException {
    for (int i = 0; i < 10; i++) {
      Result fsExistenceResult = Utils.execAsResult(LOGGER, "blkid", "-o", "value", "-s", "TYPE", dev);
      if (fsExistenceResult.exitCode == 0) {
        return;
      }
      LOGGER.info("Waiting for device {} to come online.", dev);
      Thread.sleep(TimeUnit.SECONDS.toMillis(3));
    }
    throw new IOException("Timeout, device " + dev + " did not come online");
  }

  public static String getIqn(String name) {
    return "iqn.2018-02.pack.hdfs." + name;
  }

  public static void iscsiDiscovery(List<TargetServerInfo> list) throws IOException {
    for (TargetServerInfo info : list) {
      Result result = Utils.execAsResult(LOGGER, "sudo", ISCSIADM, "-m", "discovery", "-t", "st", "-p",
          info.getHostname());
      if (result.exitCode != 0) {
        LOGGER.error("Error during discovery of server {} {}", info.getHostname(), result.stderr);
      }
    }
  }
}
