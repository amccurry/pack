package pack.zk.utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtils {

  private final static Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);

  public static final int ANY_VERSION = -1;

  public static class ConnectionWatcher implements Watcher {

    private String _zkConnectionString;
    private int _sessionTimeout;

    public void setZkConnectionString(String zkConnectionString) {
      _zkConnectionString = zkConnectionString;
    }

    public void setSessionTimeout(int sessionTimeout) {
      _sessionTimeout = sessionTimeout;
    }

    @Override
    public void process(WatchedEvent event) {
      KeeperState state = event.getState();
      LOGGER.debug("ZooKeeper {} timeout {} changed to {} state", _zkConnectionString, _sessionTimeout, state);
    }

  }

  public static void sleep(Object o) {
    try {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    } catch (InterruptedException e) {
      return;
    }
  }

  public static ZooKeeperLockManager newZooKeeperLockManager(ZooKeeperClientFactory zk, String lockPath)
      throws IOException {
    return new ZooKeeperLockManager(zk, lockPath);
  }

  private static ZooKeeperClient newZooKeeper(String zkConnectionString, int sessionTimeout) throws IOException {
    if (zkConnectionString.contains("/")) {
      int indexOf = zkConnectionString.indexOf('/');
      try (ZooKeeperClient zooKeeper = newZooKeeper(zkConnectionString.substring(0, indexOf), sessionTimeout)) {
        mkNodesStr(zooKeeper, zkConnectionString.substring(indexOf));
      }
    }
    ConnectionWatcher watcher = new ConnectionWatcher();
    watcher.setSessionTimeout(sessionTimeout);
    watcher.setZkConnectionString(zkConnectionString);
    return new ZooKeeperClient(zkConnectionString, sessionTimeout, watcher);
  }

  public static void mkNodesStr(ZooKeeper zk, String path) {
    if (path == null) {
      return;
    }
    mkNodes(zk, path.split("/"));
  }

  public static void mkNodes(ZooKeeper zk, String... path) {
    if (path == null) {
      return;
    }
    for (int i = 0; i < path.length; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j <= i; j++) {
        if (!path[j].isEmpty()) {
          builder.append('/');
          builder.append(path[j]);
        }
      }
      String pathToCheck = removeDupSeps(builder.toString());
      if (pathToCheck.isEmpty()) {
        continue;
      }
      try {
        if (zk.exists(pathToCheck, false) == null) {
          zk.create(pathToCheck, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (NodeExistsException e) {
        // do nothing
      } catch (KeeperException e) {
        LOGGER.error("error", e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOGGER.error("error", e);
        throw new RuntimeException(e);
      }
    }
  }

  private static String removeDupSeps(String path) {
    return path.replace("//", "/");
  }

  public static String getPath(String... parts) {
    if (parts == null || parts.length == 0) {
      return null;
    }
    StringBuilder builder = new StringBuilder(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      builder.append('/');
      builder.append(parts[i]);
    }
    return builder.toString();
  }

  public static boolean exists(ZooKeeper zk, String... path) {
    if (path == null || path.length == 0) {
      return true;
    }
    StringBuilder builder = new StringBuilder(path[0]);
    for (int i = 1; i < path.length; i++) {
      builder.append('/')
             .append(path[i]);
    }
    try {
      return zk.exists(builder.toString(), false) != null;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void deleteAnyVersion(ZooKeeper zk, String path) {
    try {
      List<String> children = zk.getChildren(path, false);
      for (String c : children) {
        deleteAnyVersion(zk, path + "/" + c);
      }
      zk.delete(path, ANY_VERSION);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NONODE) {
        return;
      }
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void rmr(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
    List<String> children = zooKeeper.getChildren(path, false);
    for (String c : children) {
      rmr(zooKeeper, path + "/" + c);
    }
    zooKeeper.delete(path, -1);
  }

  public static ZooKeeperClientFactory newZooKeeperClientFactory(String zkConnectionString, int sessionTimeout) {
    AtomicReference<ZooKeeperClient> _ref = new AtomicReference<ZooKeeperClient>();
    Thread thread = new Thread(() -> IOUtils.cleanup(LOGGER, _ref.get()));
    Runtime runtime = Runtime.getRuntime();
    ZooKeeperClientFactory zkcf = new ZooKeeperClientFactory() {

      private final AtomicLong _age = new AtomicLong();

      @Override
      public synchronized ZooKeeperClient getZk() throws IOException {
        while (true) {
          ZooKeeperClient zk = _ref.get();
          if (zk == null || zk.isExpired() || !zk.isConnected()) {
            if (zk != null) {
              zk.close();
            }
            _ref.set(zk = ZkUtils.newZooKeeper(zkConnectionString, sessionTimeout));
            _age.set(System.nanoTime());
          }
          if (isClientOld()) {
            _ref.set(zk = ZkUtils.reconnect(zk, zkConnectionString));
            _age.set(System.nanoTime());
          }
          if (zk.isConnected()) {
            return zk;
          } else {
            try {
              Thread.sleep(TimeUnit.SECONDS.toMillis(3));
            } catch (InterruptedException e) {
              LOGGER.warn("unknown error", e);
            }
          }
        }
      }

      private boolean isClientOld() {
        return _age.get() + (sessionTimeout / 2) < System.nanoTime();
      }

      @Override
      public void close() throws IOException {
        IOUtils.cleanup(LOGGER, _ref.get());
        try {
          runtime.removeShutdownHook(thread);
        } catch (IllegalStateException e) {
          LOGGER.warn(e.getMessage());
        }
      }
    };

    runtime.addShutdownHook(thread);
    return zkcf;
  }

  protected static ZooKeeperClient reconnect(ZooKeeperClient zk, String zkConnectionString) throws IOException {
    long sessionId = zk.getSessionId();
    byte[] sessionPasswd = zk.getSessionPasswd();
    int sessionTimeout = zk.getSessionTimeout();
    ConnectionWatcher watcher = new ConnectionWatcher();
    watcher.setSessionTimeout(sessionTimeout);
    watcher.setZkConnectionString(zkConnectionString);
    ZooKeeperClient newZk = new ZooKeeperClient(zkConnectionString, sessionTimeout, watcher, sessionId, sessionPasswd);
    zk.disconnect();
    return newZk;
  }

}