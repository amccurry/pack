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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperLockManager implements Closeable {

  private static final Callable<?> LOST_LOCK_DEFAULT = () -> null;
  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperLockManager.class);
  private static final String PID = ManagementFactory.getRuntimeMXBean()
                                                     .getName();

  protected final Map<String, ZkLockInfo> _lockMap = new ConcurrentHashMap<>();
  protected final String _lockPath;
  protected final long _timeout;
  protected final List<String> _metaData;
  protected final ZooKeeperClientFactory _zk;
  protected final ExecutorService _service;
  protected final AtomicBoolean _running = new AtomicBoolean(true);
  protected final Future<?> _future;
  protected final Callable<?> _lostLock;
  protected final AtomicLong _lastValidationCheckSessionId = new AtomicLong();

  public ZooKeeperLockManager(ZooKeeperClientFactory zk, String lockPath) throws IOException {
    this(zk, lockPath, LOST_LOCK_DEFAULT);
  }

  public ZooKeeperLockManager(ZooKeeperClientFactory zk, String lockPath, Callable<?> lostLock) throws IOException {
    _zk = zk;
    ZooKeeperClient client = zk.getZk();
    if (isConnected(client)) {
      _lastValidationCheckSessionId.set(client.getSessionId());
    } else {
      throw new IOException("Client not connected.");
    }
    _lostLock = lostLock;
    _service = Executors.newSingleThreadExecutor();
    _future = _service.submit(getValidationRunnable());
    ZkUtils.mkNodesStr(client, lockPath);
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

  protected void checkForSeesionChange() throws IOException, InterruptedException, KeeperException {
    if (isSessionIdDifferent()) {
      validateExistingLocks();
    }
  }

  protected boolean isSessionIdDifferent() throws IOException {
    ZooKeeperClient zk = _zk.getZk();
    if (isConnected(zk)) {
      return _lastValidationCheckSessionId.get() != zk.getSessionId();
    }
    return false;
  }

  protected boolean isConnected(ZooKeeperClient zk) {
    try {
      return zk.isConnected();
    } catch (InterruptedException e) {
      return false;
    }
  }

  public synchronized void validateExistingLocks() throws IOException, InterruptedException, KeeperException {
    for (Entry<String, ZkLockInfo> e : new ArrayList<>(_lockMap.entrySet())) {
      String name = e.getKey();
      ZkLockInfo lockInfo = e.getValue();
      validateExistingLock(name, lockInfo);
    }
  }

  protected void validateExistingLock(String name, ZkLockInfo lockInfo)
      throws IOException, InterruptedException, KeeperException {
    ZooKeeperClient zk = _zk.getZk();
    long sessionId = zk.getSessionId();
    if (!isConnected(zk)) {
      return;
    }
    if (lockInfo.getSesssionId() == sessionId) {
      return;
    } else {
      LOGGER.warn("Attempting to reobtain lock {}, again after session loss", name);
      unlockInternal(name);
      if (!tryToLockInternal(name)) {
        LOGGER.error("Lost lock on {}", name);
        try {
          _lostLock.call();
        } catch (Exception e) {
          throw new IOException(e);
        }
      } else {
        LOGGER.info("Success reobtain lock {}, again after session loss", name);
      }
    }
  }

  @Override
  public void close() {
    _running.set(false);
    _future.cancel(true);
    _service.shutdownNow();
  }

  public int getNumberOfLockNodesPresent(String name) throws InterruptedException, KeeperException, IOException {
    checkForSeesionChange();
    return getNumberOfLockNodesPresentInternal(name);
  }

  protected int getNumberOfLockNodesPresentInternal(String name)
      throws InterruptedException, KeeperException, IOException {
    TRY_AGAIN: while (true) {
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
          continue TRY_AGAIN;
        }
        throw e;
      }
    }
  }

  public void unlock(String name) throws InterruptedException, KeeperException, IOException {
    checkForSeesionChange();
    unlockInternal(name);
  }

  protected synchronized void unlockInternal(String name) throws InterruptedException, KeeperException, IOException {
    if (!_lockMap.containsKey(name)) {
      throw new RuntimeException("Lock [" + name + "] has not be created.");
    }
    ZkLockInfo lockInfo = _lockMap.remove(name);
    LOGGER.debug("Unlocking on path {} with name {}", lockInfo, name);
    ZooKeeperClient zk = _zk.getZk();
    try {
      zk.delete(lockInfo.getPath(), -1);
    } catch (KeeperException e) {
      if (zk.isExpired()) {
        LOGGER.warn("ZooKeeper client expired while trying to unlock {} for path {}", name, _lockPath);
        return;
      }
      throw e;
    }
  }

  public boolean checkLockOwnership(String name) throws InterruptedException, KeeperException, IOException {
    checkForSeesionChange();
    return checkLockOwnershipInternal(name);
  }

  protected synchronized boolean checkLockOwnershipInternal(String name)
      throws InterruptedException, KeeperException, IOException {
    if (!_lockMap.containsKey(name)) {
      return false;
    }
    ZooKeeperClient zk = _zk.getZk();
    try {
      ZkLockInfo lockInfo = _lockMap.get(name);
      List<String> children = getOnlyThisLocksChildren(name, zk.getChildren(_lockPath, false));
      if (hasMixOfNegativeAndPositiveNumbers(children)) {
        List<Entry<String, Long>> childrenWithVersion = lookupVersions(children, zk, _lockPath);
        children = orderLocksWithVersion(childrenWithVersion);
      } else {
        orderLocks(children);
      }
      String firstElement = children.get(0);
      if ((_lockPath + "/" + firstElement).equals(lockInfo.getPath())) {
        // yay!, we have the lock
        LOGGER.debug("Lock on path {} with name {}", _lockPath, name);
        return true;
      }
      return false;
    } catch (KeeperException e) {
      if (zk.isExpired()) {
        LOGGER.warn("ZooKeeper client expired while trying to unlock {} for path {}", name, _lockPath);
        return false;
      }
      throw e;
    }
  }

  public boolean tryToLock(String name) throws InterruptedException, KeeperException, IOException {
    checkForSeesionChange();
    return tryToLockInternal(name);
  }

  private synchronized boolean tryToLockInternal(String name)
      throws InterruptedException, KeeperException, IOException {
    if (_lockMap.containsKey(name)) {
      return false;
    }
    TRY_AGAIN: while (true) {
      ZooKeeperClient zk = _zk.getZk();
      try {
        String newPath = zk.create(_lockPath + "/" + name + "_", toString(_metaData), Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        _lockMap.put(name, ZkLockInfo.builder()
                                     .path(newPath)
                                     .sesssionId(zk.getSessionId())
                                     .build());
        List<String> children = getOnlyThisLocksChildren(name, zk.getChildren(_lockPath, false));
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
          ZkLockInfo lockInfo = _lockMap.remove(name);
          zk.delete(lockInfo.getPath(), -1);
          return false;
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

  protected List<Entry<String, Long>> lookupVersions(List<String> children, ZooKeeperClient zk, String path)
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

  protected static boolean hasMixOfNegativeAndPositiveNumbers(List<String> locks) {
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

  protected static long getUnsignedEndingNumber(String s) {
    long number = getEndingNumber(s);
    return getUnsignedLong(number);
  }

  protected static long getEndingNumber(String s) {
    int lastIndexOf = s.lastIndexOf('_');
    if (lastIndexOf < 0) {
      throw new RuntimeException("Missing ending number from " + s);
    }
    return Long.parseLong(s.substring(lastIndexOf + 1));
  }

  protected static long getUnsignedLong(long x) {
    return x & 0x0fffffffffffffffL;
  }

  protected byte[] toString(List<String> metaData) {
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

  protected List<String> getOnlyThisLocksChildren(String name, List<String> children) {
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

  protected Runnable getValidationRunnable() {
    return () -> {
      while (_running.get()) {
        try {
          validateExistingLocks();
        } catch (Throwable t) {
          LOGGER.error("Unknown error", t);
        }
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException t) {
          if (!_running.get()) {
            return;
          }
          LOGGER.error("Unknown error", t);
        }
      }
    };
  }
}