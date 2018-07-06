package pack.zk.utils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
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
  protected final ZooKeeperClientFactory _zk;
  protected final Watcher _watcher = event -> {
    synchronized (_lock) {
      _lock.notify();
    }
  };

  public ZooKeeperLockManager(ZooKeeperClientFactory zk, String lockPath) {
    _zk = zk;
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

  public int getNumberOfLockNodesPresent(String name) throws InterruptedException, KeeperException, IOException {
    while (true) {
      ZooKeeperClient zk = _zk.getZk();
      try {
        List<String> children = zk.getChildren(_lockPath, false);
        int count = 0;
        for (String s : children) {
          if (s.startsWith(name + "_")) {
            count++;
          }
        }
        return count;
      } catch (KeeperException e) {
        if (zk.isExpired()) {
          LOGGER.warn("ZooKeeper client expired while trying to getNumberOfLockNodesPresent {} for path {}", name,
              _lockPath);
        }
        throw e;
      }
    }
  }

  public synchronized void unlock(String name) throws InterruptedException, KeeperException, IOException {
    if (!_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    String lockPath = _lockMap.remove(name);
    LOGGER.debug("Unlocking on path {} with name {}", lockPath, name);
    ZooKeeperClient zk = _zk.getZk();
    try {
      zk.delete(lockPath, -1);
    } catch (KeeperException e) {
      if (zk.isExpired()) {
        LOGGER.warn("ZooKeeper client expired while trying to unlock {} for path {}", name, _lockPath);
        return;
      }
      throw e;
    }
  }

  public synchronized boolean tryToLock(String name) throws InterruptedException, KeeperException, IOException {
    if (_lockMap.containsKey(name)) {
      return false;
    }
    TRY_AGAIN: while (true) {
      ZooKeeperClient zk = _zk.getZk();
      try {
        String newPath = zk.create(_lockPath + "/" + name + "_", toString(_metaData), Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        _lockMap.put(name, newPath);
        synchronized (_lock) {
          List<String> children = getOnlyThisLocksChildren(name, zk.getChildren(_lockPath, _watcher));
          if (hasMixOfNegativeAndPositiveNumbers(children)) {
            List<Entry<String, Long>> childrenWithVersion = lookupVersions(children, zk, _lockPath);
            children = orderLocksWithVersion(childrenWithVersion);
          } else {
            orderLocks(children);
          }
          String firstElement = children.get(0);
          if ((_lockPath + "/" + firstElement).equals(newPath)) {
            // yay!, we got the lock
            LOGGER.debug("Lock on path {} with name {}", _lockPath, name);
            return true;
          } else {
            LOGGER.debug("Waiting for lock on path {} with name {}", _lockPath, name);
            String path = _lockMap.remove(name);
            zk.delete(path, -1);
            return false;
          }
        }
      } catch (KeeperException e) {
        if (zk.isExpired()) {
          LOGGER.warn("ZooKeeper client expired while trying to unlock {} for path {}", name, _lockPath);
          Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          continue TRY_AGAIN;
        }
        throw e;
      }
    }
  }

  private List<Entry<String, Long>> lookupVersions(List<String> children, ZooKeeperClient zk, String path)
      throws KeeperException, InterruptedException {
    List<Entry<String, Long>> result = new ArrayList<>();
    for (String s : children) {
      Stat stat = zk.exists(path + "/" + s, false);
      if (stat != null) {
        long czxid = stat.getCzxid();
        result.add(newEntry(s, czxid));
      }
    }
    return result;
  }

  public static List<String> orderLocksWithVersion(List<Entry<String, Long>> locks) {
    Collections.sort(locks, new Comparator<Entry<String, Long>>() {
      @Override
      public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
        return Long.compare(o1.getValue(), o2.getValue());
      }
    });
    List<String> list = new ArrayList<>();
    for (Entry<String, Long> e : locks) {
      list.add(e.getKey());
    }
    return list;
  }

  public static void orderLocks(List<String> locks) {
    if (hasMixOfNegativeAndPositiveNumbers(locks)) {
      throw new IllegalArgumentException("Can not handle mix of positive and negative ids.");
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

  public static Entry<String, Long> newEntry(String node, long version) {
    return new Entry<String, Long>() {

      @Override
      public Long setValue(Long value) {
        return null;
      }

      @Override
      public Long getValue() {
        return version;
      }

      @Override
      public String getKey() {
        return node;
      }
    };
  }
}