package pack.zk.utils;

import java.io.Closeable;
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperClient extends ZooKeeper implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClient.class);
  private static final int CONNECTED_RETRY = 10;
  private final int _internalSessionTimeout;
  private final AtomicBoolean _expired = new AtomicBoolean();

  static {
    System.setProperty(ZooKeeperSaslClient.ENABLE_CLIENT_SASL_KEY, Boolean.FALSE.toString());
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    _internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
      throws IOException {
    super(connectString, sessionTimeout, watcher, canBeReadOnly);
    _internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
      byte[] sessionPasswd, boolean canBeReadOnly) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd, canBeReadOnly);
    _internalSessionTimeout = sessionTimeout;
  }

  public ZooKeeperClient(String connectString, int sessionTimeout, Watcher watcher, long sessionId,
      byte[] sessionPasswd) throws IOException {
    super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
    _internalSessionTimeout = sessionTimeout;
  }

  public boolean isExpired() {
    return _expired.get();
  }

  static abstract class ZKExecutor<T> {
    String _name;

    ZKExecutor(String name) {
      _name = name;
    }

    abstract T execute() throws KeeperException, InterruptedException;
  }

  public <T> T execute(ZKExecutor<T> executor) throws KeeperException, InterruptedException {
    final long timestmap = System.currentTimeMillis();
    int sessionTimeout = getSessionTimeout();
    if (sessionTimeout == 0) {
      sessionTimeout = _internalSessionTimeout;
    }
    while (true) {
      try {
        return executor.execute();
      } catch (KeeperException e) {
        if (e.code() == Code.CONNECTIONLOSS && timestmap + sessionTimeout >= System.currentTimeMillis()) {
          LOGGER.warn("Connection loss");
          ZkUtils.sleep(this);
          continue;
        }
        _expired.set(true);
        throw e;
      }
    }
  }

  @Override
  public String create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<String>("create") {
      @Override
      String execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - create {} {} {} {}", path, data, acl, createMode);
        return ZooKeeperClient.super.create(path, data, acl, createMode);
      }
    });
  }

  @Override
  public void delete(final String path, final int version) throws InterruptedException, KeeperException {
    execute(new ZKExecutor<Void>("delete") {
      @Override
      Void execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - delete {} {}", path, version);
        ZooKeeperClient.super.delete(path, version);
        return null;
      }
    });
  }

  @Override
  public List<OpResult> multi(final Iterable<Op> ops) throws InterruptedException, KeeperException {
    return execute(new ZKExecutor<List<OpResult>>("multi") {
      @Override
      List<OpResult> execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.multi(ops);
      }
    });
  }

  @Override
  public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("exists") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - exists {} {}", path, watcher);
        return ZooKeeperClient.super.exists(path, watcher);
      }
    });
  }

  @Override
  public Stat exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("exists") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - exists {} {}", path, watch);
        return ZooKeeperClient.super.exists(path, watch);
      }
    });
  }

  @Override
  public byte[] getData(final String path, final Watcher watcher, final Stat stat)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<byte[]>("getData") {
      @Override
      byte[] execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getData {} {} {} ", path, watcher, stat);
        return ZooKeeperClient.super.getData(path, watcher, stat);
      }
    });
  }

  @Override
  public byte[] getData(final String path, final boolean watch, final Stat stat)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<byte[]>("getData") {
      @Override
      byte[] execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getData {} {} {}", path, watch, stat);
        return ZooKeeperClient.super.getData(path, watch, stat);
      }
    });
  }

  @Override
  public Stat setData(final String path, final byte[] data, final int version)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("setData") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - setData {} {} {}", path, data, version);
        return ZooKeeperClient.super.setData(path, data, version);
      }
    });
  }

  @Override
  public List<ACL> getACL(final String path, final Stat stat) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<ACL>>("getACL") {
      @Override
      List<ACL> execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.getACL(path, stat);
      }
    });
  }

  @Override
  public Stat setACL(final String path, final List<ACL> acl, final int version)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<Stat>("setACL") {
      @Override
      Stat execute() throws KeeperException, InterruptedException {
        return ZooKeeperClient.super.setACL(path, acl, version);
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final Watcher watcher)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getChildren {} {}", path, watcher);
        return ZooKeeperClient.super.getChildren(path, watcher);
      }

      @Override
      public String toString() {
        return "path=" + path + " watcher=" + watcher;
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getChildren {} {}", path, watch);
        return ZooKeeperClient.super.getChildren(path, watch);
      }

      @Override
      public String toString() {
        return "path=" + path + " watch=" + watch;
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final Watcher watcher, final Stat stat)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getChildren {} {} {}", path, watcher, stat);
        return ZooKeeperClient.super.getChildren(path, watcher, stat);
      }
    });
  }

  @Override
  public List<String> getChildren(final String path, final boolean watch, final Stat stat)
      throws KeeperException, InterruptedException {
    return execute(new ZKExecutor<List<String>>("getChildren") {
      @Override
      List<String> execute() throws KeeperException, InterruptedException {
        LOGGER.debug("ZK Call - getChildren {} {} {}", path, watch, stat);
        return ZooKeeperClient.super.getChildren(path, watch, stat);
      }
    });
  }

  @Override
  public synchronized void close() {
    try {
      super.close();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void disconnect() {
    cnxn.disconnect();
  }

  public boolean isConnected() throws InterruptedException {
    for (int i = 0; i < CONNECTED_RETRY; i++) {
      try {
        exists("/", false);
        return true;
      } catch (KeeperException | InterruptedException e) {
        LOGGER.warn("error while trying to check is zk is connected {}", e.getMessage());
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
      }
    }
    return false;
  }

}