package pack.util;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorUtil.class);

  public static Executor getCallerRunExecutor() {
    return command -> {
      try {
        command.run();
      } catch (Throwable t) {
        LOGGER.error(t.getMessage(), t);
      }
    };
  }

}
