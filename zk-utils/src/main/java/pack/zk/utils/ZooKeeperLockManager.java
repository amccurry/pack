package pack.zk.utils;

import java.io.Closeable;
import java.io.IOException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperLockManager implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperLockManager.class);

  protected final Map<String, LockSession> _lockMap = new ConcurrentHashMap<>();
  protected final String _lockPath;
  protected final Object _lock = new Object();
  protected final long _timeout;
  protected final Watcher _watcher = new Watcher() {
    @Override
    public void process(WatchedEvent event) {
      synchronized (_lock) {
        _lock.notify();
      }
    }
  };
  protected final String _zkConnectionString;
  protected final int _zkSessionTimeout;

  private static class LockSession implements Closeable {
    private String _lockPath;
    private ZooKeeperClient _zk;

    LockSession(String lockPath, ZooKeeperClient zk) {
      _lockPath = lockPath;
      _zk = zk;
    }

    @Override
    public void close() {
      _zk.close();
    }

  }

  public ZooKeeperLockManager(String zkConnectionString, int zkSessionTimeout, String lockPath) {
    _zkConnectionString = zkConnectionString;
    _zkSessionTimeout = zkSessionTimeout;
    _lockPath = lockPath;
    _timeout = TimeUnit.SECONDS.toMillis(1);
  }

  public int getNumberOfLockNodesPresent(String name) throws KeeperException, InterruptedException {
    try (ZooKeeperClient zooKeeper = getZk()) {
      List<String> children = zooKeeper.getChildren(_lockPath, false);
      int count = 0;
      for (String s : children) {
        if (s.startsWith(name + "_")) {
          count++;
        }
      }
      return count;
    }
  }

  private ZooKeeperClient getZk() {
    try {
      return ZkUtils.newZooKeeper(_zkConnectionString, _zkSessionTimeout);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void unlock(String name) throws InterruptedException, KeeperException {
    if (!_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    try (LockSession lockSession = _lockMap.remove(name)) {
      LOGGER.debug("Unlocking on path {} with name {}", lockSession._lockPath, name);
      lockSession._zk.delete(lockSession._lockPath, -1);
    }
  }

  public synchronized boolean tryToLock(String name) throws KeeperException, InterruptedException {
    return tryToLock(name, TimeUnit.NANOSECONDS, 0);
  }

  public synchronized boolean tryToLock(String name, TimeUnit timeUnit, long time)
      throws KeeperException, InterruptedException {
    if (_lockMap.containsKey(name)) {
      return false;
    }
    ZooKeeperClient zooKeeper = getZk();
    try {
      String newPath = zooKeeper.create(_lockPath + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);
      _lockMap.put(name, new LockSession(newPath, zooKeeper));
      long totalWaitTime = timeUnit.toNanos(time);
      long start = System.nanoTime();
      while (true) {
        synchronized (_lock) {
          List<String> children = getOnlyThisLocksChildren(name, zooKeeper.getChildren(_lockPath, _watcher));
          Collections.sort(children);
          String firstElement = children.get(0);
          if ((_lockPath + "/" + firstElement).equals(newPath)) {
            // yay!, we got the lock
            LOGGER.debug("Lock on path {} with name {}", _lockPath, name);
            return true;
          } else {
            LOGGER.debug("Waiting for lock on path {} with name {}", _lockPath, name);
            long millis = timeUnit.toMillis(time);
            if (millis > 0) {
              _lock.wait(millis);
            }
            if (start + totalWaitTime < System.nanoTime()) {
              zooKeeper.delete(newPath, -1);
              LockSession lockSession = _lockMap.remove(name);
              if (lockSession != null) {
                lockSession.close();
              }
              return false;
            }
          }
        }
      }
    } catch (KeeperException | InterruptedException e) {
      zooKeeper.close();
      throw e;
    }
  }

  public synchronized void lock(String name) throws KeeperException, InterruptedException {
    ZooKeeperClient zooKeeper = getZk();
    try {
      String newPath = zooKeeper.create(_lockPath + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);
      _lockMap.put(name, new LockSession(newPath, zooKeeper));
      while (true) {
        synchronized (_lock) {
          List<String> children = getOnlyThisLocksChildren(name, zooKeeper.getChildren(_lockPath, _watcher));
          Collections.sort(children);
          String firstElement = children.get(0);
          if ((_lockPath + "/" + firstElement).equals(newPath)) {
            // yay!, we got the lock
            LOGGER.debug("Lock on path {} with name {}", _lockPath, name);
            return;
          } else {
            LOGGER.debug("Waiting for lock on path {} with name {}", _lockPath, name);
            _lock.wait(_timeout);
          }
        }
      }
    } catch (KeeperException | InterruptedException e) {
      zooKeeper.close();
      throw e;
    }
  }

  private List<String> getOnlyThisLocksChildren(String name, List<String> children) {
    List<String> result = new ArrayList<String>();
    for (String c : children) {
      if (c.startsWith(name + "_")) {
        result.add(c);
      }
    }
    return result;
  }

  @Override
  public void close() {
    for (LockSession lockSession : _lockMap.values()) {
      lockSession.close();
    }
  }
}