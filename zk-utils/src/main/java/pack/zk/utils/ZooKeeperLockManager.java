package pack.zk.utils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperLockManager implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperLockManager.class);
  private static final String PID = ManagementFactory.getRuntimeMXBean()
                                                     .getName();

  protected final Map<String, String> _lockMap = new ConcurrentHashMap<>();
  protected final String _lockPath;
  protected final Object _lock = new Object();
  protected final long _timeout;
  protected final List<String> _metaData;
  protected final ZooKeeper _zk;
  protected final Watcher _watcher = event -> {
    synchronized (_lock) {
      _lock.notify();
    }
  };
  protected final boolean _closeZk;

  public ZooKeeperLockManager(ZooKeeper zk, String lockPath) {
    this(zk, lockPath, false);
  }

  public ZooKeeperLockManager(ZooKeeper zk, String lockPath, boolean closeZk) {
    _zk = zk;
    ZkUtils.mkNodesStr(zk, lockPath);
    _closeZk = closeZk;
    _lockPath = lockPath;
    _timeout = TimeUnit.SECONDS.toMillis(1);
    _metaData = new ArrayList<>();
    try {
      _metaData.add(PID);
      InetAddress localHost = InetAddress.getLocalHost();
      _metaData.add(localHost.getHostName());
      _metaData.add(localHost.getHostAddress());
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public int getNumberOfLockNodesPresent(String name) throws KeeperException, InterruptedException {
    List<String> children = _zk.getChildren(_lockPath, false);
    int count = 0;
    for (String s : children) {
      if (s.startsWith(name + "_")) {
        count++;
      }
    }
    return count;
  }

  public synchronized void unlock(String name) throws InterruptedException, KeeperException {
    if (!_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    String lockPath = _lockMap.remove(name);
    LOGGER.debug("Unlocking on path {} with name {}", lockPath, name);
    _zk.delete(lockPath, -1);
  }

  public synchronized boolean tryToLock(String name) throws KeeperException, InterruptedException {
    return tryToLock(name, TimeUnit.NANOSECONDS, 0);
  }

  public synchronized boolean tryToLock(String name, TimeUnit timeUnit, long time)
      throws KeeperException, InterruptedException {
    if (_lockMap.containsKey(name)) {
      return false;
    }
    String newPath = _zk.create(_lockPath + "/" + name + "_", toString(_metaData), Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    _lockMap.put(name, newPath);
    long totalWaitTime = timeUnit.toNanos(time);
    long start = System.nanoTime();
    while (true) {
      synchronized (_lock) {
        List<String> children = getOnlyThisLocksChildren(name, _zk.getChildren(_lockPath, _watcher));
        orderLocks(children);
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
            String path = _lockMap.remove(name);
            _zk.delete(path, -1);
            return false;
          }
        }
      }
    }
  }

  public synchronized void lock(String name) throws KeeperException, InterruptedException {
    String newPath = _zk.create(_lockPath + "/" + name + "_", null, Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    _lockMap.put(name, newPath);
    while (true) {
      synchronized (_lock) {
        List<String> children = getOnlyThisLocksChildren(name, _zk.getChildren(_lockPath, _watcher));
        orderLocks(children);
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
  }

  public static void orderLocks(List<String> locks) {
    if (hasMixOfNegativeAndPositiveNumbers(locks)) {
      removePositiveNumbers(locks);
    }
    Collections.sort(locks, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        long l1 = getUnsignedEndingNumber(o1);
        long l2 = getUnsignedEndingNumber(o2);
        return Long.compare(l1, l2);
      }
    });
  }

  private static void removePositiveNumbers(List<String> locks) {
    Iterator<String> iterator = locks.iterator();
    while (iterator.hasNext()) {
      String s = iterator.next();
      long endingNumber = getEndingNumber(s);
      if (endingNumber >= 0) {
        iterator.remove();
      }
    }
  }

  private static boolean hasMixOfNegativeAndPositiveNumbers(List<String> locks) {
    boolean neg = false;
    boolean pos = false;
    for (String s : locks) {
      long endingNumber = getEndingNumber(s);
      if (endingNumber < 0) {
        neg = true;
      } else {
        pos = true;
      }
    }
    return neg && pos;
  }

  private static long getUnsignedEndingNumber(String s) {
    long number = getEndingNumber(s);
    return getUnsignedLong(number);
  }

  private static long getEndingNumber(String s) {
    int lastIndexOf = s.lastIndexOf('_');
    if (lastIndexOf < 0) {
      throw new RuntimeException("Missing ending number from " + s);
    }
    return Long.parseLong(s.substring(lastIndexOf + 1));
  }

  private static long getUnsignedLong(long x) {
    return x & 0x0fffffffffffffffL;
  }

  @Override
  public void close() {
    if (_closeZk) {
      try {
        _zk.close();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private byte[] toString(List<String> metaData) {
    if (metaData == null) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (PrintWriter printWriter = new PrintWriter(out)) {
      for (String m : metaData) {
        printWriter.println(m);
      }
    }
    return out.toByteArray();
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
}