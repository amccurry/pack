package pack.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecUtil.class);

  public static void exec(Logger logger, LogLevel level, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    log(logger, level, "Executing command id {} cmd {}", uuid, list);
    Result result;
    try {
      result = exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      log(logger, level, "Command id {} complete", uuid);
    }
    if (result.exitCode != 0) {
      throw new IOException("Unknown error while trying to run command " + Arrays.asList(command));
    }
  }

  public static Result execAsResult(Logger logger, LogLevel level, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    log(logger, level, "Executing command id {} cmd {}", uuid, list);
    try {
      return exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      log(logger, level, "Command id {} complete", uuid);
    }
  }

  public static int execReturnExitCode(Logger logger, LogLevel level, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    log(logger, level, "Executing command id {} cmd {}", uuid, list);
    Result result;
    try {
      result = exec(uuid, list, logger);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      log(logger, level, "Command id {} complete", uuid);
    }
    return result.exitCode;
  }

  public static Process execAsInteractive(Logger logger, LogLevel level, String... command) throws IOException {
    String uuid = UUID.randomUUID()
                      .toString();
    List<String> list = Arrays.asList(command);
    log(logger, level, "Executing command id {} cmd {}", uuid, list);

    ProcessBuilder builder = new ProcessBuilder(list);
    return builder.start();
  }

  public static Result exec(String cmdId, List<String> command, Logger logger)
      throws IOException, InterruptedException {
    return exec(cmdId, command, logger, false);
  }

  public static Result exec(String cmdId, List<String> command, Logger logger, boolean quietly)
      throws IOException, InterruptedException {
    ProcessBuilder builder = new ProcessBuilder(command);
    Process process = builder.start();
    StringWriter stdout = new StringWriter();
    StringWriter stderr = new StringWriter();
    Thread t1 = captureOutput(cmdId, "stdout", toBuffer(process.getInputStream()), logger, stdout, quietly);
    Thread t2 = captureOutput(cmdId, "stderr", toBuffer(process.getErrorStream()), logger, stderr, quietly);
    t1.start();
    t2.start();
    int exitCode = process.waitFor();
    t1.join();
    t2.join();
    return new Result(exitCode, stdout.toString(), stderr.toString());
  }

  public static BufferedReader toBuffer(InputStream inputStream) {
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  public static Thread captureOutput(String cmdId, String type, BufferedReader reader, Logger logger, Writer writer,
      boolean quietly) {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          String s;
          while ((s = reader.readLine()) != null) {
            if (quietly) {
              logger.info("Command {} Type {} Message {}", cmdId, type, s.trim());
            } else {
              logger.debug("Command {} Type {} Message {}", cmdId, type, s.trim());
            }
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

  public static void log(Logger logger, LogLevel level, String msg, Object... arguments) {
    switch (level) {
    case DEBUG:
      logger.debug(msg, arguments);
      break;
    case ERROR:
      logger.error(msg, arguments);
      break;
    case INFO:
      logger.info(msg, arguments);
      break;
    case TRACE:
      logger.trace(msg, arguments);
      break;
    case WARN:
      logger.warn(msg, arguments);
      break;
    default:
      break;
    }
  }

  public static void log(Logger logger, LogLevel level, String msg, Throwable t) {
    switch (level) {
    case DEBUG:
      logger.debug(msg, t);
      break;
    case ERROR:
      logger.error(msg, t);
      break;
    case INFO:
      logger.info(msg, t);
      break;
    case TRACE:
      logger.trace(msg, t);
      break;
    case WARN:
      logger.warn(msg, t);
      break;
    default:
      break;
    }
  }

}
