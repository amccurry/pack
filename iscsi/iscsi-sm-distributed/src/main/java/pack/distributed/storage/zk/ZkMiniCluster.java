package pack.distributed.storage.zk;

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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkMiniCluster {

  private final static Logger LOGGER = LoggerFactory.getLogger(ZkMiniCluster.class);
  private Thread serverThread;
  private volatile ZooKeeperServerMainEmbedded zooKeeperServerMain;

  public static void main(String[] args) throws InterruptedException {
    File file = new File(args[0]);
    file.mkdirs();
    ZkMiniCluster zkMiniCluster = new ZkMiniCluster();
    zkMiniCluster.startZooKeeper(file.getAbsolutePath(), false);
    while (true) {
      Thread.sleep(1000000);
    }
  }

  public String getZkConnectionString() {
    long s = System.nanoTime();
    while (zooKeeperServerMain == null) {
      long now = System.nanoTime();
      if (s + TimeUnit.SECONDS.toNanos(60) < now) {
        throw new RuntimeException("ZooKeeper server did not start.");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return zooKeeperServerMain.getConnectionString();
  }

  public void startZooKeeper(String path) {
    startZooKeeper(true, path, false);
  }

  public void startZooKeeper(String path, boolean randomPort) {
    startZooKeeper(true, path, randomPort);
  }

  public void startZooKeeper(boolean format, String path) {
    startZooKeeper(format, path, false);
  }

  public void startZooKeeper(boolean format, String path, boolean randomPort) {
    Properties properties = new Properties();
    properties.setProperty("tickTime", "2000");
    properties.setProperty("initLimit", "10");
    properties.setProperty("syncLimit", "5");

    properties.setProperty("clientPort", "21810");

    startZooKeeper(properties, format, path, randomPort);
  }

  public void startZooKeeper(Properties properties, String path) {
    startZooKeeper(properties, true, path, false);
  }

  public void startZooKeeper(Properties properties, String path, boolean randomPort) {
    startZooKeeper(properties, true, path, randomPort);
  }

  private class ZooKeeperServerMainEmbedded extends ZooKeeperServerMain {
    @Override
    public void shutdown() {
      super.shutdown();
    }

    public String getConnectionString() {
      try {
        Field field = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        field.setAccessible(true);
        ServerCnxnFactory serverCnxnFactory = (ServerCnxnFactory) field.get(this);
        InetSocketAddress address = serverCnxnFactory.getLocalAddress();
        if (address == null) {
          return null;
        }
        int localPort = serverCnxnFactory.getLocalPort();
        return address.getAddress()
                      .getHostAddress()
            + ":" + localPort;
      } catch (NullPointerException e) {
        return null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void startZooKeeper(final Properties properties, boolean format, String path, final boolean randomPort) {
    String realPath = path + "/zk_test";
    properties.setProperty("dataDir", realPath);
    final ServerConfig serverConfig = new ServerConfig();
    QuorumPeerConfig config = new QuorumPeerConfig() {
      @Override
      public InetSocketAddress getClientPortAddress() {
        InetSocketAddress clientPortAddress = super.getClientPortAddress();
        if (randomPort) {
          return randomPort(clientPortAddress);
        }
        return clientPortAddress;
      }

      private InetSocketAddress randomPort(InetSocketAddress clientPortAddress) {
        return new InetSocketAddress(clientPortAddress.getAddress(), 0);
      }
    };
    try {
      config.parseProperties(properties);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    } catch (ConfigException e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
    serverConfig.readFrom(config);
    rm(new File(realPath));
    serverThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          zooKeeperServerMain = new ZooKeeperServerMainEmbedded();
          zooKeeperServerMain.runFromConfig(serverConfig);
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    });
    serverThread.start();
    long s = System.nanoTime();
    while (s + 10000000000L > System.nanoTime()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage(), e);
        throw new RuntimeException(e);
      }
      try {
        String zkConnectionString = getZkConnectionString();
        if (zkConnectionString == null) {
          continue;
        }
        ZooKeeper zk = new ZooKeeper(getZkConnectionString(), 30000, new Watcher() {
          @Override
          public void process(WatchedEvent event) {

          }
        });
        zk.close();
        break;
      } catch (IOException e) {
        LOGGER.error(e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        LOGGER.error(e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
  }

  public void shutdownZooKeeper() {
    if (zooKeeperServerMain != null) {
      zooKeeperServerMain.shutdown();
    }
  }

  private static void rm(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        rm(f);
      }
    }
    file.delete();
  }

}