package pack.zk.utils;

import java.io.IOException;

public interface ZooKeeperClientFactory {
  
  ZooKeeperClient getZk() throws IOException;

}
