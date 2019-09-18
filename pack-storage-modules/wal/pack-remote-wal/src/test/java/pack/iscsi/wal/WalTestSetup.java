package pack.iscsi.wal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalTestSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(WalTestSetup.class);

  private static final AtomicBoolean SETUP = new AtomicBoolean(false);
  private static CuratorFramework CURATOR_FRAMEWORK;

  public synchronized static void setup() throws Exception {
    if (!isSetup()) {

      RetryPolicy retryPolicy = new RetryForever((int) TimeUnit.SECONDS.toMillis(10));
      CURATOR_FRAMEWORK = CuratorFrameworkFactory.newClient(TestProperties.getZooKeeperConnection(), retryPolicy);
      CURATOR_FRAMEWORK.getUnhandledErrorListenable()
                       .addListener((message, e) -> {
                         LOGGER.error("Unknown error " + message, e);
                       });
      CURATOR_FRAMEWORK.getConnectionStateListenable()
                       .addListener((c, newState) -> {
                         LOGGER.info("Connection state {}", newState);
                       });
      CURATOR_FRAMEWORK.start();
      SETUP.set(true);
    }
  }

  public static CuratorFramework getCuratorFramework() throws Exception {
    setup();
    return CURATOR_FRAMEWORK;
  }

  private static boolean isSetup() {
    return SETUP.get();
  }

}
