package pack.iscsi.bk;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BKTestSetup {

  private static final Logger LOGGER = LoggerFactory.getLogger(BKTestSetup.class);

  private static final AtomicBoolean SETUP = new AtomicBoolean(false);
  private static CuratorFramework CURATOR_FRAMEWORK;
  private static BookKeeper BOOK_KEEPER;

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

      ClientConfiguration configuration = new ClientConfiguration().setMetadataServiceUri(
          TestProperties.getMetadataServiceUri());
      BOOK_KEEPER = new BookKeeper(configuration);

      SETUP.set(true);
    }
  }

  public static CuratorFramework getCuratorFramework() throws Exception {
    setup();
    return CURATOR_FRAMEWORK;
  }

  public static BookKeeper getBookKeeper() throws Exception {
    setup();
    return BOOK_KEEPER;
  }

  private static boolean isSetup() {
    return SETUP.get();
  }

}
